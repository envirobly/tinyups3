# tinyups3

## Usage

```sh
tinyups3 [--partSize=MB] [--inputSize=bytes] s3://bucket/key
  -inputSize int
        Exact input size in bytes (required)
  -partSize int
        Part size in MB for multipart upload (min 5MB) (default 5)
```

### Example

```sh
cat largefile | tinyups3 --inputSize $(stat -c%s largefile) s3://... 
```

## Building (Linux arm64)

```sh
GOOS=linux GOARCH=arm64 go build -o dist/arm64/tinyups3 .
du -sh dist/arm64/tinyups3 
```

## Development

### Formatting

Apply Go formatting:

```sh
go fmt ./...
```

### Running in development

```sh
# Install dependencies
go mod tidy

go run main.go ...
```

