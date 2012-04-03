CC=g++
CFLAGS=-c -Wall -g
LDFLAGS=
SOURCES=vc_event.cpp vc_nodes.cpp main.cpp
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=vc_run

all: $(SOURCES) $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $(EXECUTABLE)

main.o: $(SOURCES)
	$(CC) $(CFLAGS) main.cpp

vc_event.o: vc_event.cpp vc_event.h
	$(CC) $(CFLAGS) vc_event.cpp

vc_nodes.o : vc_nodes.cpp vc_nodes.h
	$(CC) $(CFLAGS) vc_nodes.cpp

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $(EXECCUTABLE)

.cpp.o:
	$(CC) $(CFLAGS) $< -o $(EXECUTABLE)

clean:
	rm -rf *o vc_run

