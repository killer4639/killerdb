package btree

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// createEmptyBTree creates a new B-tree with memory-based storage
func createEmptyBTree() *BTree {
	pages := make(map[uint64]BNode)
	var counter uint64 = 1

	return &BTree{
		Get: func(ptr uint64) BNode {
			node, ok := pages[ptr]
			if !ok {
				panic("invalid node pointer")
			}
			log.Printf("DEBUG: Getting node with ptr=%d\n", ptr)
			return node
		},
		New: func(node BNode) uint64 {
			ptr := counter
			counter++
			pages[ptr] = node
			log.Printf("DEBUG: Created new node with ptr=%d\n", ptr)
			return ptr
		},
		Del: func(ptr uint64) {
			delete(pages, ptr)
			log.Printf("DEBUG: Deleted node with ptr=%d\n", ptr)
		},
	}
}

func Main() {
	// Set up logging
	log.SetFlags(log.Ltime | log.Lshortfile)

	tree := createEmptyBTree()
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("B-tree Command Line Interface")
	fmt.Println("Available commands:")
	fmt.Println("  insert <key> <value>")
	fmt.Println("  delete <key>")
	fmt.Println("  get <key>")
	fmt.Println("  exit")
	fmt.Println()

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("ERROR: Reading input: %v\n", err)
			continue
		}

		// Trim whitespace and split the input
		input = strings.TrimSpace(input)
		parts := strings.Fields(input)

		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])
		log.Printf("DEBUG: Executing command: %s\n", command)

		switch command {
		case "exit":
			fmt.Println("Goodbye!")
			return

		case "insert":
			if len(parts) < 3 {
				fmt.Println("Usage: insert <key> <value>")
				continue
			}
			key := []byte(parts[1])
			value := []byte(strings.Join(parts[2:], " "))
			log.Printf("DEBUG: Inserting key='%s' value='%s'\n", parts[1], value)
			tree.Insert(key, value)
			fmt.Printf("Inserted key '%s' with value '%s'\n", parts[1], value)

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := []byte(parts[1])
			log.Printf("DEBUG: Deleting key='%s'\n", parts[1])
			if deleted := tree.Delete(key); deleted {
				fmt.Printf("Deleted key '%s'\n", parts[1])
			} else {
				fmt.Printf("Key '%s' not found\n", parts[1])
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := []byte(parts[1])
			log.Printf("DEBUG: Getting key='%s'\n", parts[1])
			if value, found := tree.Get(key); found {
				fmt.Printf("Value for key '%s': %s\n", parts[1], string(value))
			} else {
				fmt.Printf("Key '%s' not found\n", parts[1])
			}

		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Available commands: insert, delete, get, exit")
		}
	}
}
