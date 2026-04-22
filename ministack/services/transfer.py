"""
AWS Transfer Family Service Emulator.
JSON-based API via X-Amz-Target: TransferService.<Operation>.

Supports:
  Servers:  CreateServer, DescribeServer, DeleteServer, ListServers
  Users:    CreateUser, DescribeUser, DeleteUser, ListUsers
  SSH Keys: ImportSshPublicKey, DeleteSshPublicKey
  SFTP:     Actual SFTP server backed by in-memory S3 state (requires asyncssh)
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import sys

from ministack.core.persistence import load_state, PERSIST_STATE, STATE_DIR
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    json_response,
    new_uuid,
    now_iso,
    get_region,
)

logger = logging.getLogger("transfer")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
SFTP_BASE_PORT = int(os.environ.get("SFTP_BASE_PORT", "12200"))
SFTP_MAX_SERVERS = int(os.environ.get("SFTP_MAX_SERVERS", "100"))

try:
    import asyncssh
    _ASYNCSSH_AVAILABLE = True
except ImportError:
    asyncssh = None  # type: ignore
    _ASYNCSSH_AVAILABLE = False

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_servers = AccountScopedDict()  # server_id -> server record
_users = AccountScopedDict()    # "{server_id}/{user_name}" -> user record

_sftp_listeners: dict = {}  # server_id -> asyncssh server
_next_port = SFTP_BASE_PORT


def reset():
    global _next_port
    _next_port = SFTP_BASE_PORT
    if _sftp_listeners:
        try:
            asyncio.get_event_loop().create_task(_stop_all_sftp_servers())
        except RuntimeError:
            pass
    _servers.clear()
    _users.clear()


def get_state():
    return copy.deepcopy({"servers": _servers, "users": _users})


def restore_state(data):
    _servers.update(data.get("servers", {}))
    _users.update(data.get("users", {}))
    try:
        asyncio.get_running_loop().create_task(_start_restored_sftp_servers())
    except RuntimeError:
        pass


_restored = load_state("transfer")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _allocate_port():
    global _next_port
    if _next_port >= SFTP_BASE_PORT + SFTP_MAX_SERVERS:
        return None
    port = _next_port
    _next_port += 1
    return port


def _make_server_id(port):
    base = new_uuid().replace("-", "")[:12]
    return f"s-{base}{port:05d}"


def _port_from_server_id(sid):
    try:
        return int(sid[-5:])
    except (ValueError, IndexError):
        return None


def _key_id():
    return "key-" + new_uuid().replace("-", "")[:17]


def _server_arn(server_id):
    return f"arn:aws:transfer:{get_region()}:{get_account_id()}:server/{server_id}"


def _user_arn(server_id, user_name):
    return f"arn:aws:transfer:{get_region()}:{get_account_id()}:user/{server_id}/{user_name}"


def _user_key(server_id, user_name):
    return f"{server_id}/{user_name}"


_SSH_KEY_PREFIXES = ("ssh-rsa", "ssh-ed25519", "ssh-dss", "ecdsa-sha2-")


def _validate_ssh_key(key_body):
    if not key_body or not isinstance(key_body, str):
        return False
    parts = key_body.strip().split()
    if len(parts) < 2:
        return False
    return any(parts[0].startswith(p) for p in _SSH_KEY_PREFIXES)


def _error(code, message, status=400):
    return error_response_json(code, message, status)


def _s3_get_bucket(bucket_name):
    """Return S3 bucket dict or None. Uses sys.modules to avoid triggering lazy load."""
    s3 = sys.modules.get("ministack.services.s3")
    if s3 is None:
        return None
    return s3._buckets.get(bucket_name)


def _s3_put_object(bucket_name, key, body):
    s3 = sys.modules.get("ministack.services.s3")
    if s3 is None:
        return False
    bucket = s3._buckets.get(bucket_name)
    if bucket is None:
        return False
    etag = f'"{hashlib.md5(body).hexdigest()}"'
    bucket["objects"][key] = {
        "body": body,
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": etag,
        "last_modified": now_iso(),
        "size": len(body),
        "metadata": {},
        "preserved_headers": {},
    }
    return True



# ---------------------------------------------------------------------------
# asyncssh server classes and S3-backed file handles
# ---------------------------------------------------------------------------

if _ASYNCSSH_AVAILABLE:
    class _S3ReadFile:
        def __init__(self, body: bytes):
            self._body = body
            self._pos = 0

        def seek(self, offset, whence=0):
            if whence == 0:
                self._pos = offset
            elif whence == 1:
                self._pos += offset
            elif whence == 2:
                self._pos = len(self._body) + offset
            return self._pos

        def read(self, size=-1):
            if size < 0:
                data = self._body[self._pos:]
                self._pos = len(self._body)
            else:
                data = self._body[self._pos:self._pos + size]
                self._pos += len(data)
            return data

        def flush(self):
            pass

        def close(self):
            pass

    class _S3WriteFile:
        def __init__(self, bucket: str, key: str, append: bool = False):
            self._bucket = bucket
            self._key = key
            self._buf = bytearray()
            self._pos = 0
            if append:
                existing = _s3_get_bucket(bucket)
                if existing:
                    obj = existing["objects"].get(key)
                    if obj:
                        self._buf = bytearray(obj["body"])
                        self._pos = len(self._buf)

        def seek(self, offset, whence=0):
            if whence == 0:
                self._pos = offset
            elif whence == 1:
                self._pos += offset
            elif whence == 2:
                self._pos = len(self._buf) + offset
            return self._pos

        def write(self, data: bytes):
            end = self._pos + len(data)
            if end > len(self._buf):
                self._buf.extend(b"\x00" * (end - len(self._buf)))
            self._buf[self._pos:end] = data
            self._pos = end
            return len(data)

        def flush(self):
            pass

        def close(self):
            _s3_put_object(self._bucket, self._key, bytes(self._buf))

    class _TransferSSHServer(asyncssh.SSHServer):
        def public_key_auth_supported(self):
            return True

        def validate_public_key(self, username, key):
            try:
                server_id, user_name = username.split("/", 1)
            except ValueError:
                return False
            user = _users.get(_user_key(server_id, user_name))
            if not user:
                return False
            for stored in user.get("SshPublicKeys", []):
                try:
                    if key == asyncssh.import_public_key(stored["SshPublicKeyBody"]):
                        return True
                except Exception:
                    continue
            return False

    class _TransferSFTPServer(asyncssh.SFTPServer):
        def __init__(self, conn):
            super().__init__(conn)
            username = conn.get_extra_info("username") or ""
            try:
                server_id, user_name = username.split("/", 1)
            except ValueError:
                raise asyncssh.SFTPError(asyncssh.FX_PERMISSION_DENIED, "Invalid username format")
            user = _users.get(_user_key(server_id, user_name))
            if not user:
                raise asyncssh.SFTPError(asyncssh.FX_PERMISSION_DENIED, "User not found")
            self._home_dir_type = user.get("HomeDirectoryType", "PATH")
            self._mappings = user.get("HomeDirectoryMappings", [])
            self._home_directory = user.get("HomeDirectory") or "/"

        # -- path translation ------------------------------------------------

        def _sftp_path_to_s3(self, sftp_path: str):
            """Translate an absolute SFTP path to (bucket, s3_key)."""
            path = "/" + sftp_path.strip("/")

            if self._home_dir_type == "PATH":
                home = self._home_directory.rstrip("/") or "/"
                full = (home + path) if path != "/" else home
                parts = full.lstrip("/").split("/", 1)
                return parts[0], (parts[1] if len(parts) > 1 else "")

            # LOGICAL: find the longest matching Entry prefix
            best = None
            for m in self._mappings:
                norm = m["Entry"].rstrip("/") or "/"
                if norm == "/":
                    if best is None:
                        best = m
                elif path == norm or path.startswith(norm + "/"):
                    if best is None or len(m["Entry"]) > len(best["Entry"]):
                        best = m

            if best is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No mapping for path: {sftp_path}")

            entry_norm = best["Entry"].rstrip("/") or "/"
            target = best["Target"].rstrip("/")

            if entry_norm == "/":
                remainder = path.lstrip("/")
                full_s3 = f"{target}/{remainder}" if remainder else f"{target}/"
            else:
                remainder = path[len(entry_norm):]  # e.g. "" or "/file.csv"
                full_s3 = target + (remainder if remainder else "/")

            parts = full_s3.lstrip("/").split("/", 1)
            bucket = parts[0]
            key = (parts[1] if len(parts) > 1 else "").strip("/")
            return bucket, key

        # -- SFTP operations -------------------------------------------------
        # asyncssh passes paths as bytes to all SFTPServer methods.
        # _sp() decodes to str for internal logic; realpath re-encodes to bytes.

        @staticmethod
        def _sp(path):
            return path.decode("utf-8") if isinstance(path, bytes) else path

        def realpath(self, path):
            path = self._sp(path)
            if not path or path == ".":
                return b"/"
            if not path.startswith("/"):
                path = "/" + path
            parts = []
            for p in path.split("/"):
                if p == "..":
                    if parts:
                        parts.pop()
                elif p and p != ".":
                    parts.append(p)
            return ("/" + "/".join(parts)).encode("utf-8")

        def _norm(self, path):
            """Decode bytes path and return normalised str."""
            return self._sp(self.realpath(path))

        def _dir_attrs(self):
            return asyncssh.SFTPAttrs(permissions=0o040755, size=0)

        def _file_attrs(self, obj):
            return asyncssh.SFTPAttrs(permissions=0o100644, size=obj["size"])

        def stat(self, path):
            return self._s3_stat(self._norm(path))

        def lstat(self, path):
            return self._s3_stat(self._norm(path))

        def _s3_stat(self, path):
            if path == "/":
                return self._dir_attrs()

            if self._home_dir_type == "LOGICAL":
                has_root = any(m["Entry"].strip("/") == "" for m in self._mappings)
                if not has_root:
                    # Validate path matches at least one mapping before S3 lookup
                    for m in self._mappings:
                        norm = m["Entry"].rstrip("/")
                        if path == norm or path.startswith(norm + "/"):
                            break
                    else:
                        raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")

            bucket, key = self._sftp_path_to_s3(path)
            bucket_data = _s3_get_bucket(bucket)
            if bucket_data is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")
            objects = bucket_data["objects"]
            if key in objects:
                return self._file_attrs(objects[key])
            prefix = (key + "/") if key else ""
            if not key or any(k.startswith(prefix) for k in objects):
                return self._dir_attrs()
            raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")

        def listdir(self, path):
            path = self._norm(path)

            # Virtual root: no mapping covers "/" so we synthesise top-level dirs
            if self._home_dir_type == "LOGICAL" and path == "/":
                has_root = any(m["Entry"].strip("/") == "" for m in self._mappings)
                if not has_root:
                    seen: set = set()
                    result = []
                    for m in self._mappings:
                        top = m["Entry"].strip("/").split("/")[0]
                        if top and top not in seen:
                            seen.add(top)
                            result.append(asyncssh.SFTPName(top.encode(), attrs=self._dir_attrs()))
                    return result

            bucket, key = self._sftp_path_to_s3(path)
            bucket_data = _s3_get_bucket(bucket)
            if bucket_data is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such directory: {path}")

            prefix = (key + "/") if key else ""
            objects = bucket_data["objects"]
            seen_dirs: set = set()
            files: dict = {}

            for obj_key in objects:
                if prefix and not obj_key.startswith(prefix):
                    continue
                remainder = obj_key[len(prefix):]
                if not remainder:
                    continue
                if "/" in remainder:
                    seen_dirs.add(remainder.split("/")[0])
                else:
                    files[remainder] = objects[obj_key]

            result = []
            for d in sorted(seen_dirs):
                result.append(asyncssh.SFTPName(d.encode(), attrs=self._dir_attrs()))
            for name, obj in sorted(files.items()):
                result.append(asyncssh.SFTPName(name.encode(), attrs=self._file_attrs(obj)))
            return result

        def open(self, path, pflags, attrs):
            path = self._norm(path)
            bucket, key = self._sftp_path_to_s3(path)

            if pflags & (asyncssh.FXF_WRITE | asyncssh.FXF_CREAT):
                return _S3WriteFile(bucket, key, append=bool(pflags & asyncssh.FXF_APPEND))

            bucket_data = _s3_get_bucket(bucket)
            if bucket_data is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")
            obj = bucket_data["objects"].get(key)
            if obj is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")
            return _S3ReadFile(obj["body"])

        def remove(self, path):
            path = self._norm(path)
            bucket, key = self._sftp_path_to_s3(path)
            bucket_data = _s3_get_bucket(bucket)
            if bucket_data is None or key not in bucket_data["objects"]:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {path}")
            del bucket_data["objects"][key]

        def rename(self, oldpath, newpath):
            oldpath = self._norm(oldpath)
            newpath = self._norm(newpath)
            old_bucket, old_key = self._sftp_path_to_s3(oldpath)
            new_bucket, new_key = self._sftp_path_to_s3(newpath)
            old_data = _s3_get_bucket(old_bucket)
            if old_data is None or old_key not in old_data["objects"]:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such file: {oldpath}")
            obj = old_data["objects"].pop(old_key)
            new_data = _s3_get_bucket(new_bucket)
            if new_data is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"Target bucket not found: {new_bucket}")
            new_data["objects"][new_key] = obj

        def mkdir(self, path, attrs):
            pass  # S3 has no real directories; implicit on object creation

        def rmdir(self, path):
            path = self._norm(path)
            bucket, key = self._sftp_path_to_s3(path)
            bucket_data = _s3_get_bucket(bucket)
            if bucket_data is None:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, f"No such directory: {path}")
            prefix = (key + "/") if key else ""
            for k in [k for k in bucket_data["objects"] if k.startswith(prefix)]:
                del bucket_data["objects"][k]

        def fstat(self, file_obj):
            if isinstance(file_obj, _S3WriteFile):
                return asyncssh.SFTPAttrs(permissions=0o100644, size=len(file_obj._buf))
            if isinstance(file_obj, _S3ReadFile):
                return asyncssh.SFTPAttrs(permissions=0o100644, size=len(file_obj._body))
            return asyncssh.SFTPAttrs(permissions=0o100644)

        def setstat(self, path, attrs):
            pass

        def statvfs(self, path):
            tb = 2 ** 40
            return asyncssh.SFTPVFSAttrs(
                bsize=4096, frsize=4096,
                blocks=tb // 4096, bfree=tb // 4096, bavail=tb // 4096,
                files=2 ** 32, ffree=2 ** 32, favail=2 ** 32,
                fsid=0, flag=0, namemax=255,
            )

else:
    _TransferSSHServer = None
    _TransferSFTPServer = None


# ---------------------------------------------------------------------------
# Host key
# ---------------------------------------------------------------------------

_host_key = None
_HOST_KEY_PATH = os.path.join(STATE_DIR, "transfer_host_key.pem")


def _get_host_key():
    global _host_key
    if _host_key is not None:
        return _host_key
    if not _ASYNCSSH_AVAILABLE:
        return None
    if PERSIST_STATE and os.path.exists(_HOST_KEY_PATH):
        try:
            _host_key = asyncssh.read_private_key(_HOST_KEY_PATH)
            return _host_key
        except Exception as e:
            logger.warning("Failed to load SFTP host key: %s", e)
    _host_key = asyncssh.generate_private_key("ssh-ed25519")
    if PERSIST_STATE:
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            _host_key.write_private_key(_HOST_KEY_PATH)
        except Exception as e:
            logger.warning("Failed to persist SFTP host key: %s", e)
    return _host_key


# ---------------------------------------------------------------------------
# SFTP server lifecycle
# ---------------------------------------------------------------------------

async def _start_sftp_server(server_id: str, port: int):
    if not _ASYNCSSH_AVAILABLE:
        return
    if server_id in _sftp_listeners:
        return
    try:
        server = await asyncssh.create_server(
            _TransferSSHServer,
            host="",
            port=port,
            server_host_keys=[_get_host_key()],
            sftp_factory=_TransferSFTPServer,
            allow_scp=False,
        )
        _sftp_listeners[server_id] = server
        logger.info("SFTP server started for %s on port %d", server_id, port)
    except Exception as e:
        logger.error("Failed to start SFTP server for %s on port %d: %s", server_id, port, e)


async def _stop_sftp_server(server_id: str):
    server = _sftp_listeners.pop(server_id, None)
    if server is None:
        return
    try:
        server.close()
        await server.wait_closed()
        logger.info("SFTP server stopped for %s", server_id)
    except Exception as e:
        logger.warning("Error stopping SFTP server for %s: %s", server_id, e)


async def _stop_all_sftp_servers():
    for sid in list(_sftp_listeners):
        await _stop_sftp_server(sid)


async def _start_restored_sftp_servers():
    for sid in list(_servers):
        port = _port_from_server_id(sid)
        if port and sid not in _sftp_listeners:
            await _start_sftp_server(sid, port)


async def stop_all_sftp_servers():
    """Called by app.py lifespan shutdown to clean up all SFTP listeners."""
    await _stop_all_sftp_servers()


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateServer": _create_server,
        "DescribeServer": _describe_server,
        "DeleteServer": _delete_server,
        "ListServers": _list_servers,
        "CreateUser": _create_user,
        "DescribeUser": _describe_user,
        "DeleteUser": _delete_user,
        "ListUsers": _list_users,
        "ImportSshPublicKey": _import_ssh_public_key,
        "DeleteSshPublicKey": _delete_ssh_public_key,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    result = handler(data)
    if asyncio.iscoroutine(result):
        return await result
    return result


# ---------------------------------------------------------------------------
# Server handlers
# ---------------------------------------------------------------------------

async def _create_server(data):
    port = _allocate_port()
    if port is None:
        return _error("ServiceUnavailableException",
                       f"SFTP server limit reached (max {SFTP_MAX_SERVERS})", 503)

    sid = _make_server_id(port)
    server = {
        "Arn": _server_arn(sid),
        "ServerId": sid,
        "State": "ONLINE",
        "EndpointType": data.get("EndpointType", "PUBLIC"),
        "IdentityProviderType": data.get("IdentityProviderType", "SERVICE_MANAGED"),
        "Protocols": data.get("Protocols", ["SFTP"]),
        "Domain": data.get("Domain", "S3"),
        "Tags": data.get("Tags", []),
        "UserCount": 0,
        "DateCreated": now_iso(),
    }
    _servers[sid] = server
    await _start_sftp_server(sid, port)
    return json_response({"ServerId": sid})


def _describe_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)
    server = _servers[sid]
    return json_response({"Server": {
        "Arn": server["Arn"],
        "Domain": server.get("Domain", "S3"),
        "EndpointType": server["EndpointType"],
        "IdentityProviderType": server["IdentityProviderType"],
        "Protocols": server["Protocols"],
        "ServerId": sid,
        "State": server["State"],
        "Tags": server.get("Tags", []),
        "UserCount": server.get("UserCount", 0),
    }})


async def _delete_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)
    for k in [k for k in _users if k.startswith(sid + "/")]:
        del _users[k]
    del _servers[sid]
    await _stop_sftp_server(sid)
    return json_response({})


def _list_servers(data):
    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")
    all_servers = sorted(_servers.values(), key=lambda s: s["ServerId"])
    start = 0
    if next_token:
        for i, s in enumerate(all_servers):
            if s["ServerId"] == next_token:
                start = i + 1
                break
    page = all_servers[start:start + max_results]
    result = {
        "Servers": [{
            "Arn": s["Arn"],
            "Domain": s.get("Domain", "S3"),
            "EndpointType": s["EndpointType"],
            "IdentityProviderType": s["IdentityProviderType"],
            "Protocols": s["Protocols"],
            "ServerId": s["ServerId"],
            "State": s["State"],
            "UserCount": s.get("UserCount", 0),
        } for s in page],
    }
    if start + max_results < len(all_servers):
        result["NextToken"] = all_servers[start + max_results]["ServerId"]
    return json_response(result)


# ---------------------------------------------------------------------------
# User handlers
# ---------------------------------------------------------------------------

def _create_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")

    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)

    uk = _user_key(sid, user_name)
    if uk in _users:
        return _error("ResourceExistsException", f"User already exists: {user_name}", 409)

    ssh_keys = []
    ssh_body = data.get("SshPublicKeyBody")
    if ssh_body:
        if not _validate_ssh_key(ssh_body):
            return _error("InvalidRequestException",
                           "Unsupported or invalid SSH public key format", 400)
        ssh_keys.append({
            "SshPublicKeyId": _key_id(),
            "SshPublicKeyBody": ssh_body.strip(),
            "DateImported": now_iso(),
        })

    home_dir_type = data.get("HomeDirectoryType", "PATH")
    mappings = data.get("HomeDirectoryMappings", [])
    home_directory = data.get("HomeDirectory")

    user = {
        "Arn": _user_arn(sid, user_name),
        "UserName": user_name,
        "ServerId": sid,
        "HomeDirectoryType": home_dir_type,
        "HomeDirectoryMappings": mappings,
        "HomeDirectory": home_directory,
        "Role": data.get("Role", ""),
        "SshPublicKeys": ssh_keys,
        "Tags": data.get("Tags", []),
    }
    _users[uk] = user
    _servers[sid]["UserCount"] = _servers[sid].get("UserCount", 0) + 1
    return json_response({"ServerId": sid, "UserName": user_name})


def _describe_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException", f"Unknown user: {user_name}", 404)
    user = _users[uk]
    described = {
        "Arn": user["Arn"],
        "HomeDirectoryType": user.get("HomeDirectoryType", "PATH"),
        "HomeDirectoryMappings": user.get("HomeDirectoryMappings", []),
        "Role": user.get("Role", ""),
        "SshPublicKeys": user.get("SshPublicKeys", []),
        "Tags": user.get("Tags", []),
        "UserName": user["UserName"],
    }
    if user.get("HomeDirectory"):
        described["HomeDirectory"] = user["HomeDirectory"]
    return json_response({"ServerId": sid, "User": described})


def _delete_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException", f"Unknown user: {user_name}", 404)
    del _users[uk]
    if sid in _servers:
        _servers[sid]["UserCount"] = max(0, _servers[sid].get("UserCount", 1) - 1)
    return json_response({})


def _list_users(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)
    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")
    prefix = sid + "/"
    server_users = sorted(
        [u for k, u in _users.items() if k.startswith(prefix)],
        key=lambda u: u["UserName"],
    )
    start = 0
    if next_token:
        for i, u in enumerate(server_users):
            if u["UserName"] == next_token:
                start = i + 1
                break
    page = server_users[start:start + max_results]
    result = {
        "ServerId": sid,
        "Users": [{
            "Arn": u["Arn"],
            "HomeDirectoryType": u.get("HomeDirectoryType", "PATH"),
            "Role": u.get("Role", ""),
            "SshPublicKeyCount": len(u.get("SshPublicKeys", [])),
            "UserName": u["UserName"],
        } for u in page],
    }
    if start + max_results < len(server_users):
        result["NextToken"] = server_users[start + max_results]["UserName"]
    return json_response(result)


# ---------------------------------------------------------------------------
# SSH key handlers
# ---------------------------------------------------------------------------

def _import_ssh_public_key(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    ssh_body = data.get("SshPublicKeyBody", "")
    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException", f"Unknown user: {user_name}", 404)
    if not _validate_ssh_key(ssh_body):
        return _error("InvalidRequestException",
                       "Unsupported or invalid SSH public key format", 400)
    kid = _key_id()
    _users[uk]["SshPublicKeys"].append({
        "SshPublicKeyId": kid,
        "SshPublicKeyBody": ssh_body.strip(),
        "DateImported": now_iso(),
    })
    return json_response({"ServerId": sid, "SshPublicKeyId": kid, "UserName": user_name})


def _delete_ssh_public_key(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    key_id = data.get("SshPublicKeyId", "")
    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException", f"Unknown user: {user_name}", 404)
    keys = _users[uk]["SshPublicKeys"]
    _users[uk]["SshPublicKeys"] = [k for k in keys if k["SshPublicKeyId"] != key_id]
    return json_response({})
