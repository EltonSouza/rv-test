package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antchfx/htmlquery"

	_ "github.com/go-sql-driver/mysql"
)

type dado struct {
	nomeAcao     string
	nomeEmpresa  string
	valorMercado int64
	oscilacaoDia string
}

type Config struct {
	Database struct {
		Host     string `json:"host"`
		User     string `json:"user"`
		Password string `json:"password"`
		Port     string `json:"port"`
	} `json:"database"`
}

func pegaDados(url string) (d dado) {
	url = "http://www.fundamentus.com.br/" + url
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return d
	}
	nAcao := htmlquery.FindOne(doc, "//span[contains(text(), 'Papel')]/../../td[2]")
	nEmpresa := htmlquery.FindOne(doc, "//span[contains(text(), 'Empresa')]/../../td[2]")
	vMercado := htmlquery.FindOne(doc, "//span[contains(text(), 'Valor de mercado')]/../../td[2]")
	oDia := htmlquery.FindOne(doc, "//span[contains(text(), 'Dia')]/../../td[2]")

	if nAcao == nil || nEmpresa == nil || vMercado == nil || oDia == nil {
		return d
	}

	vlMercado := htmlquery.InnerText(vMercado)
	vlMercado = strings.Replace(vlMercado, ".", "", -1)
	vlMercadoInt, _ := strconv.ParseInt(vlMercado, 10, 64)

	osDia := htmlquery.InnerText(oDia)
	osDia = strings.Replace(osDia, "%", "", 1)
	osDia = strings.Replace(osDia, ",", ".", 1)

	d = dado{
		nomeAcao:     htmlquery.InnerText(nAcao),
		nomeEmpresa:  htmlquery.InnerText(nEmpresa),
		valorMercado: vlMercadoInt,
		oscilacaoDia: osDia,
	}
	return d
}

func main() {

	url := "https://www.fundamentus.com.br/detalhes.php"
	resp, _ := http.Get(url)
	html, _ := ioutil.ReadAll(resp.Body)

	r := regexp.MustCompile(`detalhes.php\?papel=[\w]*`)
	urlsList := r.FindAllStringSubmatch(string(html), -1)

	var wg sync.WaitGroup
	wg.Add(len(urlsList))
	// wg.Add(21)

	dados := []dado{}

	fmt.Println("Coletando dados...")
	c := make(chan dado, 100)
	for _, u := range urlsList {
		time.Sleep(100 * time.Millisecond)
		go func(u string, c chan dado) {
			rDado := pegaDados(u)
			fmt.Println(rDado)
			wg.Done()
			c <- rDado
		}(u[0], c)
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	for n := range c {
		dados = append(dados, n)
	}

	sort.Slice(dados, func(i, j int) bool {
		return dados[j].valorMercado < dados[i].valorMercado
	})

	config := LoadConfiguration("config.json")
	db, err := sql.Open("mysql", ""+config.Database.User+":"+config.Database.Password+"@tcp("+config.Database.Host+":"+config.Database.Port+")/")
	if err != nil {
		fmt.Println("Error: ", err)
		panic(err.Error())
	}
	defer db.Close()

	// Create Database if not exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS redventures_test")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("USE redventures_test")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS papeisranking (
    		id INT NOT NULL AUTO_INCREMENT,
    		nomeAcao varchar(100) NOT NULL,
   			nomeEmpresa varchar(100) NOT NULL,
    		oscilacaoDiaria FLOAT NOT NULL,
    		valorMercado FLOAT NOT NULL,
    		groupId VARCHAR(15) NOT NULL,
    		CONSTRAINT papeisranking_PK PRIMARY KEY (id)
		);`)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare(`
			insert into papeisranking(
				nomeAcao, nomeEmpresa,
				oscilacaoDiaria, valorMercado,groupId
			) values (?, ?, ?, ?, ?)`)

	t := time.Now()

	for i := 0; i <= 9; i++ {
		_, err = stmt.Exec(dados[i].nomeAcao, dados[i].nomeEmpresa, dados[i].oscilacaoDia, dados[i].valorMercado, t.Format("20060102150405"))
		if err != nil {
			log.Print(err)
		}
	}
}

func LoadConfiguration(file string) Config {
	var config Config
	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	return config
}
