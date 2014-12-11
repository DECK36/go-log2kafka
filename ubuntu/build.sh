#! /bin/bash -ex

TMP=`mktemp -d`
export GOPATH=$TMP
go get github.com/DECK36/go-log2kafka
go build github.com/DECK36/go-log2kafka

cp $TMP/bin/go-log2kafka ./log2kafka

fpm -s dir -t deb --verbose \
	-n deck36-log2kafka --version 0.1 --iteration 1 \
	--url https://github.com/DECK36/go-log2kafka \
	--maintainer "Martin Schuette <martin.schuette@deck36.de>" \
	--prefix /usr/local/bin \
	--deb-default log2kafka.default \
	--deb-upstart log2kafka.upstart \
	log2kafka

