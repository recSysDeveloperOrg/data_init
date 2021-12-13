#!/bin/bash
a=1
while [ $a -ne 0 ]; do
    go run ./main.go
    a=$?
done