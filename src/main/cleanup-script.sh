#!/usr/bin/env bash
set -xe

sort mr-out* | grep . > mr-test-output
rm mr-out*
rm pg-*-[0-9]*
diff mr-correct-wc-txt mr-test-output
