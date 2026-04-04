package main

import (
  "context"
  "encoding/json"
  "fmt"
  "os"

  "fugue/internal/sourceimport"
)

func main() {
  imp := sourceimport.New(sourceimport.Config{})
  stack, err := imp.InspectGitHubCompose(context.Background(), sourceimport.GitHubComposeInspectRequest{RepoURL: "https://github.com/yym68686/uni-api"})
  if err != nil {
    panic(err)
  }
  enc := json.NewEncoder(os.Stdout)
  enc.SetIndent("", "  ")
  for _, svc := range stack.Services {
    if svc.Name == "uni-api" {
      fmt.Println("SERVICE")
      if err := enc.Encode(svc); err != nil {
        panic(err)
      }
    }
  }
}
