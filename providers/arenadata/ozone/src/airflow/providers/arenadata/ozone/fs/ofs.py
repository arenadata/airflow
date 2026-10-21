from __future__ import annotations

import io
import os
import subprocess
import tempfile

from fsspec import AbstractFileSystem
from airflow.sdk.bases.hook import BaseHook


class OzoneFsFileSystem(AbstractFileSystem):
    """
    Minimal fsspec filesystem over native Ozone CLI (ozone fs / RootedOzoneFileSystem)
    In PoC only read/write via PyFileSystem(FSSpecHandler(...)) is supported
    """

    protocol = "ofs"

    def __init__(self, conn_id: str | None = None, conf_dir: str = "/opt/ozone-conf", **kwargs):
        super().__init__(**kwargs)
        self._conf_dir = conf_dir
        self._om_address = "ozone"
        self._env = os.environ.copy()
        if conn_id is not None:
            conn = BaseHook.get_connection(conn_id)
            self._om_address = f"{conn.host or 'ozone'}:{conn.port or 9862}"
            self._env["OZONE_OM_ADDRESS"] = self._om_address

    def _run(self, *args: str) -> str:
        result = subprocess.run(
            ["ozone", "--config", self._conf_dir, "fs",
             "-D", "ozone.replication=ONE", "-D", "ozone.replication.type=RATIS",
             *args],
            capture_output=True, text=True, env=self._env,
        )
        if result.returncode != 0:
            raise FileNotFoundError(result.stderr.strip())
        return result.stdout.strip()

    def _strip_uri(self, path: str) -> str:
        """Return plain /vol/bucket/key path."""
        # Strip leading slashes before checking for URI (handles //ozoneofs://... case)
        stripped = path.lstrip("/")
        if "://" in stripped:
            from urllib.parse import urlsplit
            p = urlsplit(stripped)
            _, _, host = p.netloc.rpartition("@")
            return f"/{host}{p.path}"
        return "/" + stripped

    def _uri(self, path: str) -> str:
        p = self._strip_uri(path)
        return f"ofs://{self._om_address}{p}"

    def ls(self, path: str, detail: bool = True, **kwargs):
        uri = self._uri(path)
        try:
            out = self._run("-ls", "-R", uri)
        except FileNotFoundError:
            return []
        results = []
        for line in out.splitlines():
            parts = line.split()
            # ozone fs -ls output: permissions links owner group size date time name
            if len(parts) < 8:
                continue
            name_uri = parts[-1]
            # convert ofs://host/vol/bucket/key -> vol/bucket/key
            if "://" in name_uri:
                from urllib.parse import urlsplit
                p = urlsplit(name_uri)
                _, _, host = p.netloc.rpartition("@")
                name = (host + p.path).lstrip("/")
            else:
                name = name_uri.lstrip("/")
            is_dir = parts[0].startswith("d")
            info = {
                "name": name,
                "size": 0 if is_dir else int(parts[4]),
                "type": "directory" if is_dir else "file",
            }
            if detail:
                results.append(info)
            else:
                results.append(name)
        return results

    def open(self, path: str, mode: str = "rb", **kwargs):
        path = self._strip_uri(path)
        if "r" in mode:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            self._run("-get", "-f", self._uri(path), tmp_path)
            data = open(tmp_path, "rb").read()
            os.unlink(tmp_path)
            return io.BytesIO(data)
        if "w" in mode or "a" in mode:
            return _OfsWriteBuffer(path, self)
        raise ValueError(f"Unsupported mode: {mode}")


class _OfsWriteBuffer(io.RawIOBase):
    def __init__(self, path: str, fs: OzoneFsFileSystem):
        self._path = path
        self._fs = fs
        self._buf = io.BytesIO()

    def write(self, data):
        return self._buf.write(data)

    def close(self):
        if not self.closed:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(self._buf.getvalue())
                tmp_path = tmp.name
            parent = str(self._fs._uri(self._path)).rsplit("/", 1)[0]
            self._fs._run("-mkdir", "-p", parent)
            self._fs._run("-put", "-f", tmp_path, self._fs._uri(self._path))
            os.unlink(tmp_path)
            super().close()

    def readable(self): return False
    def writable(self): return True
    def seekable(self): return False

