install:
	sudo python setup.py install --record log.txt
	
uninstall:
	sudo cat log.txt | sudo xargs rm -rf 

clean:
	sudo rm -rf build
	rm -f *.pyc
	rm -f log.txt
	rm -f ness.event

cleandb:
	rm -rf ndbs
