#!/bin/bash
for i in {1..10}; do
	curl -X POST "http://localhost:8000/send?msg=msg$i"
done
