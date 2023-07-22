package generate

import (
	"math/rand"
	"os"
	"strconv"
	"time"
)

func Generate(amount int) error {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)

	f, err := os.Create("input.txt")
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 0; i < amount; i++ {
		f.WriteString(strconv.Itoa(r.Intn(9000000)) + "\n")
	}

	return nil
}
