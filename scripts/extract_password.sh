#!/bin/sh
cat setup-password.out  | grep "PASSWORD $1" | cut -d'=' -f2 | colrm 1 2
