.PHONY: all

all:
	@sync && echo "echo 3 > /proc/sys/vm/drop_caches" | sudo sh && ./main.py write --batched
	@sync && echo "echo 3 > /proc/sys/vm/drop_caches" | sudo sh && ./main.py write
	@sync && echo "echo 3 > /proc/sys/vm/drop_caches" | sudo sh && ./main.py read --batched
	@sync && echo "echo 3 > /proc/sys/vm/drop_caches" | sudo sh && ./main.py read
