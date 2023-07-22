package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)

var tepmPath = func(index int) string {
	return fmt.Sprintf("temp/%d.txt", index)
}

func main() {
	start := time.Now()
	//создаем потоки для создания чанков, их сортировки и сохранения
	countFiles := <-initChunks("input.txt", 1000000)
	//массив сканеров для чтения данных из файла
	chunkScannerFiles := make([]*bufio.Scanner, countFiles)
	//очередь с приоритетом
	pq := make(PriorityQueue, countFiles)
	//итератор для очереди
	iter := 0
	count := 0

	for i := 0; i < countFiles; i++ {
		buffFile, _ := os.Open(tepmPath(i + 1))
		defer buffFile.Close()

		chunkScannerFiles[i] = bufio.NewScanner(buffFile)
		chunkScannerFiles[i].Scan()

		if firstValue, err := strconv.Atoi(chunkScannerFiles[i].Text()); err == nil {
			pq[i] = &Item{
				value:     firstValue, //первое значение в файле
				fileIndex: i,          //номер файла
				index:     iter,       //по умолчанию(требуется для очереди)
			} //записываем структуру с первым значением и номером каждого файла в очередь
		}
		iter++
	}
	//инициализируем очередь
	heap.Init(&pq)

	outFile, _ := os.Create("output.txt")

	for count != iter {
		//получаем первую структуру из очереди(с минимальным значением)
		item := heap.Pop(&pq).(*Item)
		//записываем значение структуры в файл
		outFile.WriteString(strconv.Itoa(item.value) + "\n")
		fi := item.fileIndex              //номер файла из структуры
		if chunkScannerFiles[fi].Scan() { //читаем сканером следующее значение файла
			if value, err := strconv.Atoi(chunkScannerFiles[fi].Text()); err == nil {
				heap.Push(&pq, &Item{ //записываем структуру со след. значением и тем же номером файла в очередь
					value:     value,
					fileIndex: fi,
					index:     iter,
				})
				iter++
			}
		}
		count++
	}
	outFile.Close()
	fmt.Printf("Execution time: %v", time.Since(start))
	fmt.Scanln()
}

func initChunks(fileName string, sizeChunk int) chan int {
	chunk := make(chan []int)
	sortedChunk := make(chan []int)

	doneParse := parseInputFileOnChunk(fileName, sizeChunk, chunk)
	doneSort := sortChunk(chunk, sortedChunk, sizeChunk, doneParse)
	countFiles := saveChunkToFile(sortedChunk, sizeChunk, doneSort)

	return countFiles
}

func sortChunk(chunkCh chan []int, sortChunkCh chan []int, sizeChunk int, done chan struct{}) chan struct{} {
	d := make(chan struct{})
	go func() {
	loop:
		for {
			select {
			case chunk := <-chunkCh:
				{
					sort.Ints(chunk)
					sortChunkCh <- copySlice(chunk, sizeChunk)
				}
			case <-done:
				d <- struct{}{}
				break loop
			}
		}
	}()

	return d
}

func saveChunkToFile(chunkCh chan []int, sizeChunk int, done chan struct{}) chan int {
	d := make(chan int)
	var iterFile = 1
	go func() {
	loop:
		for {
			select {
			case chunk := <-chunkCh:
				{
					outFile, _ := os.Create(tepmPath(iterFile))
					for _, v := range chunk {
						outFile.WriteString(strconv.Itoa(v) + "\n")
					}
					outFile.Close()

					iterFile++
				}
			case <-done:
				d <- iterFile - 1
				break loop
			}
		}
	}()

	return d
}

func parseInputFileOnChunk(fileName string, sizeChunk int, chunkCh chan []int) chan struct{} {
	done := make(chan struct{})
	go func() {

		inputFile, _ := os.Open(fileName)
		defer inputFile.Close()

		scanner := bufio.NewScanner(inputFile)
		iterChunk := 0
		chunk := make([]int, sizeChunk)

		for scanner.Scan() {
			if stringToInt, err := strconv.Atoi(scanner.Text()); err == nil {
				chunk[iterChunk] = stringToInt
			}
			iterChunk++
			if iterChunk == sizeChunk {
				iterChunk = 0
				chunkCh <- copySlice(chunk, sizeChunk)
			}
		}

		done <- struct{}{}
	}()

	return done
}

func copySlice[T any](inputSlice []T, size int) []T {
	buffSlice := make([]T, size)
	copy(buffSlice, inputSlice)
	return buffSlice
}
