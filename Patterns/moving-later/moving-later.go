package movinglater

type Database interface {
	Query(query string) string
}

func DistributedQuery(replicas []Database, query string) string {
	responseCh := make(chan string, 1)
	for _, replica := range replicas {
		go func() {
			select {
			case responseCh <- replica.Query(query):
			default:
				return
			}
		}()
	}

	return <-responseCh
}
