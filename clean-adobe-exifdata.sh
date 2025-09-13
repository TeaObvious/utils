#!/usr/bin/env bash
exiftool -adobe:all= -xmp:all= -photoshop:all= -tagsfromfile @ -iptc:all -overwrite_original *jpg */*jpg
