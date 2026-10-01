# VektorFS

Fault-tolerant distributed file system: Client -> Tracker -> Storage Nodes,
asyncio + a framed TCP protocol (4-byte length + JSON), SHA-256 end to end.
Runtime has no third-party dependencies; `requirements.txt` is for tests only.

## Run locally (PowerShell)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pytest                      # all tests
python -m pytest -m "not e2e"         # skip the real-process test

python -m tracker.server --port 9100 --ttl 10 --metadata-file .\data\meta.json
1..4 | % { Start-Process python "-m node.server --port $(9000+$_) --storage-dir .\data\node$_ --node-id node$_ --tracker 127.0.0.1:9100 --advertise-host 127.0.0.1" }
python -m client.cli upload .\file.bin
python -m client.cli status
python -m client.cli download file.bin .\out.bin
```

## Docker
```powershell
docker compose up -d --build
mkdir transfer; copy file.bin transfer\
docker compose run --rm client upload /transfer/file.bin
docker compose kill node2                         # crash a node
docker compose run --rm client status             # after ~10s: node2 DEAD, chunks re-replicated
docker compose run --rm client download file.bin /transfer/out.bin
docker compose start node2                        # re-registers, keeps its volume
```
