# Makefile for plaunch
CC = mpicc
BIN = plaunch
FLAGS = -o
DEFINES = 

all: build install
build:
	$(CC) $(FLAGS) $(BIN) plaunch.c $(DEFINES)
install: 
	cp -v $(BIN) ../bin
clean:
	rm *.o $(BIN)
