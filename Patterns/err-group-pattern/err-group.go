package errgrouppattern

import (
	"context"
	"sync"
)

// Да, тут типо пример БДшки
type Database interface {
	Query(query string) (string, error)
}

// Я вот иногда на чужой код смотрю и думаю: "Какой я долбоеб", - ну да ладно
// Если не знаешь - узнай. Кроче, мы тут эмулируем сборку данных из шардов (про шарды я знаю, что они 1400 стоят и продают их только после 15 минуты)
func DistributedQuery(shards []Database, query string) []string {
	//Вейтгруппа, это типа объект, который ждет выполнеия процессов хз зачем мне знать определения клссов, если я знаю как они работают
	var wg sync.WaitGroup
	//Да, собстнна, добавляем в группу кол-во элементов = шардам
	wg.Add(len(shards))

	//Канал с оветами
	responseCh := make(chan string)
	//Цикл проходки по шардам
	for _, shard := range shards {
		go func() {
			defer wg.Done()
			//ВЫполянем запрос
			response, _ := shard.Query(query)
			//Пишем в чанлу
			responseCh <- response
		}()
	}

	//Бля, вот это крутая штука, чтоб горутинка ожидала завершения всех других и закрывала канлу
	//Скажите - база всем знать надо, я скажу - люто заларпил такой паттерн вчера увидел на Хабре
	go func() {
		wg.Wait()
		close(responseCh)
	}()
	//Создаем массив с ответами, куда будем кидать данные с шардов
	responses := make([]string, 0, len(shards))
	for response := range responseCh {
		responses = append(responses, response)
	}
	//Возвращаем ответы
	return responses
}

// Не, ну это новый уровень, по сути своей все библиотеки на Го вы можете мягко интегрировать в свои проекты понимая как они просто устроены
// Все они зиждятся на одних о тех же примитивах
type ErrGrop struct {
	cancel func(error)    //Опа, анти-папа
	wg     sync.WaitGroup //вейтгруппа

	errOnce sync.Once //это тоже синка
	err     error     //и ошибка, все просто по сути, но если вы лентя, то всегда есть библиотека ErrGroup сама по себе
}

// Создаем новую группу ошибок
func NewErrGroup(ctx context.Context) (*ErrGrop, context.Context) {
	//Мы передаем контекст, это важно для получения ошибки
	ctx, cancel := context.WithCancelCause(ctx)
	return &ErrGrop{cancel: cancel}, ctx
}

// Собстнна по сути это просто горутина, которая выполняет действие и выдает ошибку
// А, ну и конечно с WG
func (g *ErrGrop) Go(action func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := action(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				g.cancel(g.err)
			})
		}
	}()
}

// Ожидание
func (g *ErrGrop) Wait() error {
	g.wg.Wait()
	return g.err
}

// Эта другая функция для сравнения, она - улучшенная версия верхней функции
func DistributedQuery2(ctx context.Context, shards []Database, query string) ([]string, error) {

	var mu sync.Mutex
	//Сразу создаем массив с ответами
	responses := make([]string, 0, len(shards))
	//Тут создаем ErrGroup, зачем они вообще нужны
	//Ну, это чистое удобство вывода ошибок, да и можно через контекст все горутинки остановить
	group, ctx := NewErrGroup(ctx)

	for _, shard := range shards {
		group.Go(func() error {
			// Прямо тут вызываем запрос, без лишней горутины и канала
			response, err := shard.Query(query)
			if err != nil {
				return err
			}

			mu.Lock()
			responses = append(responses, response)
			mu.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	} else {
		return responses, nil
	}
}
