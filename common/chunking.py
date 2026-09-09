from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024


def chunk_file(file_path: str, output_dir: str) -> list[Path]:
    
    source = Path(file_path)
    destination = Path(output_dir)

    if not source.is_file():
        raise FileNotFoundError(f"File not found: {source}")

    destination.mkdir(parents=True, exist_ok=True)

    chunks = []

    with source.open("rb") as file:
        index = 0

        while True:
            data = file.read(CHUNK_SIZE)

            if not data:
                break

            chunk_path = destination / f"chunk_{index:06d}"
            chunk_path.write_bytes(data)

            chunks.append(chunk_path)
            index += 1

    return chunks


def reassemble_file(chunk_paths: list[str], output_file: str,) -> Path:

    output = Path(output_file)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("wb") as destination:
        for chunk_path in chunk_paths:
            chunk = Path(chunk_path)

            if not chunk.is_file():
                raise FileNotFoundError(f"Chunk not found: {chunk}")

            with chunk.open("rb") as source:
                while data := source.read(1024 * 1024):
                    destination.write(data)

    return output