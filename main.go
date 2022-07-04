package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bsm/extsort"
	"github.com/customerio/homework/stream"
)

type Attr struct {
	Value     string
	Timestamp int64
}

type userAttrMap map[string]Attr
type userEventMap map[string]map[string]bool

type User struct {
	ID       string
	AttrMap  userAttrMap
	EventMap userEventMap
}

type usersMap map[string]User


var output = flag.String("out", "data.csv", "output file")
var input = flag.String("in", "", "input file")
var verify = flag.String("verify", "", "verification file")
var cleanRun = flag.String("cleanrun", "", "y for a clean rerun")

func main() {

	// Args 0: program
	// 1:filename to be processed
	// 2:verify file
	// 3:cleanRunFlag y - optional

	flag.Parse()
	

	fileName := input
	verifyFile := verify
	cleanRunFlag := cleanRun
	

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Memory/CPU profiling
	cpufile, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}

	// Heap memory profiling
	err = pprof.WriteHeapProfile(cpufile)
	if err != nil {
		panic(err)
	}

	defer cpufile.Close()
	defer pprof.StopCPUProfile()
	log.Println("Start processing the file")

	// Check if the processing can be resumed
	statusFile, chunkStatusMap := loadResumeStatus(*cleanRunFlag, *fileName)
	//log.Println(chunkStatusMap)

	// Counter to capture number of file chunks
	nChunks := int64(0)

	// Offset variable to read file in chunks
	var offsetStart int64
	var wg sync.WaitGroup
	
	// number of threads to restrict memory usage
	numThreads := make(chan int, 4)

	// Variable to check end of file
	eof := false
	for {
		if eof {
			break
		}
		var err error
		var buf []byte
		chunkOffset := offsetStart
		log.Println("Get Next Chunk of data for offset: ", chunkOffset)
		buf, offsetStart, err = getNextChunk(*fileName, offsetStart)
		if err == io.EOF {
			eof = true
		}
		// Nothing to process
		if len(buf) == 0 {
			continue
		}
		nChunks++

		// check if the chunk has already been processed
		if chunkStatusMap[strconv.FormatInt(chunkOffset, 10)] != "" {
			log.Println("Already processed this offset:", chunkOffset)
			continue
		}

		numThreads <- 1
		wg.Add(1)
		var lock sync.Mutex
		// processing chunks concurrently. Map phase of the program
		go func() {
			result, err := ProcessChunk(ctx, bytes.NewReader(buf))
			if err != nil {
				fmt.Println("Error:", err)
				panic(err)
				return
			}

			// serailize the chunks output
			offsetFile, err := encodeMapFile(*fileName, chunkOffset, result)

			lock.Lock()
			// Make an entry in status file.
			// If the processing fails/interrupted in between, this file will be reused to resume the processing
			// This needs to be synchronized to prevent garbled entries
			_, err1 := statusFile.WriteString(strconv.FormatInt(chunkOffset, 10) + "," + offsetFile + "\n")
			if err1 != nil {
				log.Fatalf("failed writing to file: %s", err1)
			}
			lock.Unlock()

			<-numThreads
			wg.Done()

		}()

		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
	}

	wg.Wait()
	statusFile.Close()
	log.Println("Bytes:", offsetStart, "Chunks:", nChunks)

	// Reduce phase of the processing.
	reduceUserMap(*fileName + ".status")

	// Validate the output with the verify file
	err2 := validate(*output, *verifyFile)
	if err2 != nil {
		fmt.Println(err2)
	} else {
		log.Println("Successfully validated")
	}
}

// sorts Attribute map alphabetically
// it returns keys in sorted order
func (m userAttrMap) sort() (index []string) {
	for k, _ := range m {
		index = append(index, k)
	}
	sort.Strings(index)
	return
}

// sorts Event map alphabetically
// it returns keys in sorted order
func (m userEventMap) sort() (index []string) {
	for k, _ := range m {
		index = append(index, k)
	}
	sort.Strings(index)
	return
}

// sorts Users map alphabetically
// it returns keys(User IDs) in sorted order.
// Using external merge sort as we have to limit memory
func (m usersMap) sort() (index []string) {
	sorter := extsort.New(nil)
	defer sorter.Close()
	for k, _ := range m {
		sorter.Append([]byte(k))
		index = append(index, k)
	}
	iter, err := sorter.Sort()
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	y := 0
	for iter.Next() {
		index[y] = string(iter.Data())
		y++
	}
	return
}

//Merge two same user map entries. This func is invoked during the reduce phase of the program
//Get latest value of attribute
//Get sum of event value
func mergeUser(user1 User, user2 User) User {
	attrMap1 := user1.AttrMap
	attrMap2 := user2.AttrMap
	eventMap1 := user1.EventMap
	eventMap2 := user2.EventMap

	for k, v := range attrMap2 {
		val := attrMap1[k]
		if val.Value != "" {
			if val.Timestamp < v.Timestamp {
				attr := Attr{v.Value, v.Timestamp}
				attrMap1[k] = attr
			}
		} else {
			attr := Attr{v.Value, v.Timestamp}
			attrMap1[k] = attr
		}
	}

	for k, v := range eventMap2 {
		val := eventMap1[k]
		if val != nil {
			for k1 := range v {
				val[k1] = true
			}
			eventMap1[k] = val
		} else {
			eventMap1[k] = v
		}

	}
	user1.EventMap = eventMap1
	user1.AttrMap = attrMap1

	return user1
}

//Merge two user maps to aggregate the common users
func mergeUserMap(userMap1 usersMap, userMap2 usersMap) usersMap {

	for k, v := range userMap2 {
		val := userMap1[k]
		if val.AttrMap == nil {
			userMap1[k] = v
		} else {
			userMap1[k] = mergeUser(val, v)
		}
	}

	return userMap1

}

//Merge record and map entry of the same user. This func is invoked during the map phase of the program
//Get latest value of attribute
//Get sum of event value.
//Refactor to remove duplicated code.
func updateUserMap(record *stream.Record, user User) User {
	attrMap := user.AttrMap
	eventMap := user.EventMap

	if record.Type == "attributes" {
		for newKey, newValue := range record.Data {
			val := attrMap[newKey]
			if val.Value != "" {
				if val.Timestamp < record.Timestamp {
					attr := Attr{newValue, record.Timestamp}
					attrMap[newKey] = attr
				}
			} else {
				attr := Attr{newValue, record.Timestamp}
				attrMap[newKey] = attr
			}
		}
	} else {
		idMap := eventMap[record.Name]
		if idMap == nil {
			idMap = make(map[string]bool)
		}
		idMap[record.ID] = true
		eventMap[record.Name] = idMap
	}
	user.AttrMap = attrMap
	user.EventMap = eventMap

	return user
}

//Deserializes gob and return userMap
func decodeFile(encodedFile string) usersMap {
	userMap := make(usersMap)
	decodeFile, err := os.Open(encodedFile)
	if err != nil {
		return nil
	}
	defer decodeFile.Close()
	decoder := gob.NewDecoder(decodeFile)
	users := make([]User, 0)
	decoder.Decode(&users)

	for _, v := range users {
		userMap[v.ID] = v
	}

	return userMap
}

// Reduce phase of the program.
// Reads the list of encoded files by index.
// Merges user records common in all the same indexed files
func reduceUserMap(statusFile string) {
	log.Println("Reduce Phase started")
	dat, err := os.OpenFile(statusFile,os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	s := bufio.NewScanner(dat)
	fileNamePrefixList := make([]string, 0)
	for s.Scan() {
		if s.Text() != "completed" {
			fileNamePrefixList = append(fileNamePrefixList, strings.Split(s.Text(), ",")[1])
		}
	}
	for i := 0; i < 10; i++ {
		var result usersMap
		for _, prefix := range fileNamePrefixList {
			userMap := decodeFile(prefix + "_" + strconv.Itoa(i) + ".gob")
			if userMap == nil {
				continue
			}
			//PrintMemUsage()

			if result == nil {
				result = userMap
			} else {
				result = mergeUserMap(result, userMap)
			}
			//PrintMemUsage()
		}
		sortAndWriteRecords(result, i)
	}
	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}

}

// Serialize each chunk into respective index files based on the userID
// Assumption userIds are integers represented as strings.
// TODO: If Users Ids are not integers, first letter ascii value can be taken as the index
func encodeMapFile(fileName string, chunkOffset int64, result usersMap) (string, error) {

	splitMap := make(map[string][]User)
	log.Println("Map Phase for Chunk offset:", chunkOffset)

	for k, v := range result {
		userID := k[0:1]
		splitUserList := splitMap[userID]
		if splitUserList == nil {
			splitUserList = make([]User, 0)
		}
		splitUserList = append(splitUserList, v)
		splitMap[userID] = splitUserList
	}

	offsetFilePrefix := fileName + "_" + strconv.FormatInt(chunkOffset, 10)
	var err error
	for k, v := range splitMap {
		offsetFile := offsetFilePrefix + "_" + k + ".gob"
		encodeFile, err := os.Create(offsetFile)
		if err != nil {
			panic(err)
		}
		encoder := gob.NewEncoder(encodeFile)
		if err := encoder.Encode(v); err != nil {
			fmt.Println("error encoding offset:", chunkOffset, k)
			panic(err)
		}

		encodeFile.Close()
	}
	return offsetFilePrefix, err
}

// Get the next chunk of bytes from the input file.
// After fixed number of bytes, go till next new line to get the complete json
func getNextChunk(fileName string, offset int64) ([]byte, int64, error) {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Error to read [file=%v]: %v", fileName, err.Error())
	}

	// TODO: Use sync.Pool to reuse
	buf := make([]byte, 0, 25*1024*1024)

	f.Seek(offset, 0)
	r := bufio.NewReader(f)
	n, err := r.Read(buf[:cap(buf)])
	buf = buf[:n]
	if n == 0 {
		return buf, offset, err
	}
	nextUntillNewline, err := r.ReadBytes('\n')

	if err != io.EOF {
		buf = append(buf, nextUntillNewline...)
		err = nil
	}
	// log.Println("start offset, return offset:, err", offset, offset + int64(len(buf)), err)
	offset += int64(len(buf))

	return buf, offset, err
}

// This func helps in resuming the process if it stops while running.
// After a chunk is processed, a entry in the status file is made and it is used to resume.
// If an entry exists in this file, chunk is already processed and serialized files are present.
func loadResumeStatus(cleanRunFlag string, fileName string) (*os.File, map[string]string) {
	var statusFile *os.File
	chunkStatusmap := make(map[string]string)
	if !strings.EqualFold(cleanRunFlag, "y") {
		if _, err := os.Stat(fileName + ".status"); err == nil {
			log.Println("status file exists:", fileName + ".status")
			statusFile, err = os.OpenFile(fileName+".status",os.O_RDWR|os.O_CREATE, 0755)
			s := bufio.NewScanner(statusFile)
			for s.Scan() {
				if s.Text() != "completed" {
					chunkStatusmap[strings.Split(s.Text(), ",")[0]] = strings.Split(s.Text(), ",")[1]
				}
			}
		} else if errors.Is(err, os.ErrNotExist) {
			statusFile, err = os.Create(fileName + ".status")
		}
	} else {
		var err error
		statusFile, err = os.Create(fileName + ".status")
		if err != nil {
			log.Fatalf("failed opening file: %s", err)
		}
	}
	return statusFile, chunkStatusmap
}

// Sort the reduced records and write to the output file
func sortAndWriteRecords(result1 usersMap, i int) {
	log.Println("Sort and write for files indexed:", i)
	var fw *os.File
	if i == 0 {
		var err error
		fw, err = os.Create(*output)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var err error
		fw, err = os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}
	}

	defer fw.Close()

	// var sortedString []string
	j := 0
	var sb strings.Builder
	for _, k := range result1.sort() {

		j++

		user := result1[k]
		sb.WriteString(k)
		if j%10000 == 0 {
			_, err2 := fw.WriteString(sb.String())
			if err2 != nil {
				log.Fatal(err2)
			}

			sb.Reset()
		}
		for _, i := range user.AttrMap.sort() {
			attr := user.AttrMap[i]
			sb.WriteString(",")
			sb.WriteString(i)
			sb.WriteString("=")
			sb.WriteString(attr.Value)
		}

		eventsExists := false
		for _, i := range user.EventMap.sort() {
			eventsExists = true
			evnt := user.EventMap[i]
			sb.WriteString(",")
			sb.WriteString(i)
			sb.WriteString("=")
			sb.WriteString(strconv.Itoa(len(evnt)))
		}
		if !eventsExists {
			sb.WriteString(",")
		}
		sb.WriteString("\n")

	}

	_, err2 := fw.WriteString(sb.String())
	if err2 != nil {
		log.Fatal(err2)
	}

}

// Get the stream of records for the input chunk
// Merge the users. This is in the map phase of the program.
func ProcessChunk(ctx context.Context, content io.ReadSeeker) (usersMap, error) {
	ch, err := stream.Process(ctx, content)
	if err != nil {
		log.Fatal(err)
	}
	result1 := make(usersMap)
	for rec := range ch {
		select {
		case <-ctx.Done():
			log.Println("Signal terminated")
			return nil, errors.New("")
		default:
			{
				if rec.UserID == "" {
					continue
				}
				user, isPresent := result1[rec.UserID]
				if !isPresent {
					user = User{rec.UserID, make(userAttrMap), make(map[string]map[string]bool)}
				}
				result1[rec.UserID] = updateUserMap(rec, user)
			}
		}

	}
	return result1, nil

}

// Quick validation of expected and received input.
func validate(have, want string) error {
	log.Println("Validating if output and verify files are same")
	f1, err := os.Open(have)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(want)
	if err != nil {
		return err
	}
	defer f2.Close()

	s1 := bufio.NewScanner(f1)
	s2 := bufio.NewScanner(f2)
	for s1.Scan() {
		if !s2.Scan() {
			return fmt.Errorf("want: insufficient data")
		}
		t1 := s1.Text()
		t2 := s2.Text()
		if t1 != t2 {
			return fmt.Errorf("have/want: difference\n%s\n%s", t1, t2)
		}
	}
	if s2.Scan() {
		return fmt.Errorf("have: insufficient data")
	}
	if err := s1.Err(); err != nil {
		return err
	}
	if err := s2.Err(); err != nil {
		return err
	}
	return nil
}

// Helper func to print the memory allocation
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
