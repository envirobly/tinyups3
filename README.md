# tinyups3

## Formatting

Apply Go formatting:

```sh
go fmt ./...
```

## Running in development

```sh
# Install dependencies
go mod tidy

echo "1" | go run main.go s3://<bucket>/1.txt
```

## Building distribution for Linux arm64

```sh
GOOS=linux GOARCH=arm64 go build -o dist/arm64/tinyups3 .
du -sh dist/arm64/tinyups3 
```
