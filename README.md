# s3-sync

Sync directories and S3 prefixes.

## Usage

    ./s3-sync \
      --image-tag registry.example.com/repository:tag \
      --sync "schedule=@every 5m,bucket=bucket1,prefix=prefix1,dst=/path/to/dir1" \
      --sync "schedule=@every 10m,bucket=bucket2,prefix=prefix2,dst=/path/to/dir2"

### Flags

    $ ./s3-sync --help
    Usage of ./s3-sync:
      -image-tag value
            Tag of a container image to build and push to a registry after sync.
      -oneshot
            Run the sync and exit.
      -stop-timeout duration
            Timeout in seconds to stop. (default 10s)
      -sync value
            Sync directories and S3 prefixes.

## Development

### Building

    go build

### Building Docker image

    docker build .
