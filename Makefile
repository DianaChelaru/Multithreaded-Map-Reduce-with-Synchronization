build:
		g++ -std=c++11 -Wall -Wextra RankSort.cpp -o RankSort -lpthread

clean:
		rm -f RankSort
