for i in {1..10}; do dd if=/dev/urandom bs=64KiB count=1 of=test_$i; done
