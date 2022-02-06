module github.com/goal-web/queue

go 1.17

require (
	github.com/goal-web/contracts v0.1.32
	github.com/goal-web/supports v0.1.13
	github.com/qbhy/parallel v1.4.0
	github.com/segmentio/kafka-go v0.4.27
)

require (
	github.com/apex/log v1.9.0 // indirect
	github.com/frankban/quicktest v1.14.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pkg/errors v0.8.1 // indirect
	golang.org/x/crypto v0.0.0-20220128200615-198e4374d7ed // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
)

replace github.com/goal-web/contracts => ../contracts
