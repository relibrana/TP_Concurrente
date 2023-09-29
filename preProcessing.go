package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {

	file, err := os.Open("OPENDATA_DS_01_2022_01_06_ATENCIONES.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	outputFile, err := os.Create("./output.csv")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	outputFile.WriteString("\xEF\xBB\xBF") 
	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(outputFile)

	rowCount := 0

	for scanner.Scan() {
		if rowCount == 0 {
			rowCount++
			continue
		}

		line := scanner.Text()
		line = keepLast3Columns(line)
		line = strings.ReplaceAll(line, ",", ";")

		line = fixBareDoubleQuotes(line)

		fmt.Fprintln(writer, line)
		rowCount++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input file:", err)
		return
	}

	writer.Flush()

	fmt.Printf("Data has been saved to output.csv with UTF-8 encoding, excluding the first row (header) which had %d rows.\n", rowCount)

	file2, err := os.Open("output.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file2.Close()
	outputFile2, err := os.Create("./output2.csv")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile2.Close()

	scanner2 := bufio.NewScanner(file2)
	writer2 := bufio.NewWriter(outputFile2)

	for scanner2.Scan() {
		line := scanner2.Text()

		fields := strings.Split(line, ",")
		for i, field := range fields {

			if strings.Contains(field, "L") {
				fields[i] = `1`

			}
			if strings.Contains(field, "F") {
				fields[i] = `0`
			}
			if strings.Contains(field, "4") {
				fields[i] = `1`

			}
			if strings.Contains(field, "5") {
				fields[i] = `2`

			}
			if strings.Contains(field, "2") {
				fields[i] = `3`

			}
			if strings.Contains(field, "8") {
				fields[i] = `4`

			}
			if strings.Contains(field, "3") {
				fields[i] = `5`

			}
			if strings.Contains(field, "6") {
				fields[i] = `6`

			}
		}
		line = strings.Join(fields, ",")

		fmt.Fprintln(writer2, line)

	}
	writer2.Flush()
	fmt.Printf("Data has been saved to output2.csv with UTF-8 encoding")
}

func fixBareDoubleQuotes(line string) string {
	fields := strings.Split(line, "|")
	for i, field := range fields {
		if strings.Contains(field, `"`) && !strings.HasPrefix(field, `"`) && !strings.HasSuffix(field, `"`) {
			fields[i] = `"` + field + `"`
		}
	}
	return strings.Join(fields, ",")
}

const GRUPO_EDAD_INDEX = 15

func keepLast3Columns(line string) string {
	fields := strings.Split(line, "|")
	if len(fields) >= 3 {
		return strings.Join(fields[len(fields)-3:], "|")
	}

	return line
}
