.PHONY: all clean debug
all: func_body_remover

CC = clang
CFLAGS = -O3
CXXFLAGS = -O3 -std=c++20
LDFLAGS = -O3 -lstdc++ -lstdc++fs

main.o: main.cc
	$(CC) -c $(CXXFLAGS) $< -o $@

tree-sitter-cpp-%.o: ~/tree-sitter-cpp/src/%.c
	$(CC) -c $(CFLAGS) $< -o $@

debug: CFLAGS += -O0 -fsanitize=address
debug: CXXFLAGS += -O0 -fsanitize=address
debug: LDFLAGS += -O0 -fsanitize=address
debug: func_body_remover

func_body_remover: main.o ~/tree-sitter-0.25.3/libtree-sitter.a tree-sitter-cpp-parser.o tree-sitter-cpp-scanner.o
	$(CC) $(LDFLAGS) $^ -o $@

clean:
	rm -f *.o func_body_remover
