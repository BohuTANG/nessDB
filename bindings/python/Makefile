clean:
	rm -f ness.event
	rm -f *.pyc
	rm -rf ndbs
	rm -rf build
	rm -rf log

write:
	mkdir log
	python ./test.py write 1000000

check:
	python ./test.py check 1000000
