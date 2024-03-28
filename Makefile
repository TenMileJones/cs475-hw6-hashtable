all: main.c ts_hashmap.o rtclock.o
	gcc -O0 -Wall -g -o hashtest main.c ts_hashmap.o rtclock.o -lpthread

ts_hashmap.o: ts_hashmap.h ts_hashmap.c
	gcc -O0 -Wall -g -c ts_hashmap.c

rtclock.o: rtclock.h rtclock.c
	gcc -O3 -Wall -g -c rtclock.c

clean:
	rm -f hashtest *.o
