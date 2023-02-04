## airflow-etl-processes

Airflow kullanılarak iki farklı `etl` süreci içeren bir repodur. Bu `etl` süreçleri olabildiğince dinamik bir yapıda yapılmıştır.

* Birinci yöntemde `extract` ve `transform` işlemlerindeki yük verinin çekildiği `source database`'e bırakılmıştır. Database'den dönüştürülmüş şekilde elde edilen veriler `target database`'e yüklenmiştir.

* İkinci yöntemde `transaction` işlemlerinin gerçekleştirildiği bir veri tabanına `transform` gibi maliyetli bir aşamayı yaptırmak `source database`'in başka isteklere cevap vermesini yavaşlatacağı için `source database`'e sadece `extract` işlemleri yaptırılmıştır. `transform` ve `load` işlemleri `source database`'den ayrı olarak yapılmıştır.

> Bütün ETL sürecindeki işlemler `pandas` kullanılarak yapılmıştır. Bu sebeple `pandas`'ın üzerinde işlem yapamadığı hiçbir şey bu programda gerçekleştirilemez. `pandas`'ın üzerinde işlem yapamadığı bir veritabanından veri extract edilemez ya da veri yazılamaz.

> Dinamik yapı Airflow `Variables` üzerinden gerçekleştirilmektedir. `source database`, `target database`, `extract` edilecek tablolar ve `load` işleminin gerçekleştirileceği tablolar, hangi tabloların `transform` işlemine tabi tutulacağı bilgileri `Variables` değerlerini barındıran `json` dosyalarında bulunmaktadır ve bu dosyaları `airflow`'e entegre edilir.

### Method 1

Bu yöntemde, transform işlemlerinin oluşturduğu yük veritabanına verilmiştir. Aşağıda `method1` sürecinin `task` akışını görmektesiniz.

> `_data_mart` ifadelerinin bulunduğu kısımlar dinamik olarak kod ile oluşturulmaktadır.

![alt text](/images/method1_tasks.png)


* `truncate_or_create`: Load işleminin yapılacağı tablo `truncate` edilmektedir. Eğer bu tablo `target database`de yoksa tablo için `create` işlemi yapılmaktadır.
* `extract_transform`: `transform` işlemi için gereken filtreleme, join gibi işlemleri barındıran bir `SQL Query`'si ile `source database`'den `transform` edilmiş veri elde edilir.
* `load`: `extract_transform` task'ından elde edilen veriyi doğrudan `target database`'e load eden task.


#### Method1'in eksikleri ya da bağımlılıkları

1. Bu method `prod` ortamdaki bir database'den veri çekerken prod'daki database'in diğer client'lara cevap verme süresini önemli ölçüde yavaşlatabilir çünkü filtreleme ve join gibi işlemler database tarafından yapılmaktadır.
2. Bu yöntemde `source database` dışında gelen bir datanın işleme tabi olması mümkün değildir.


## Method2

Bu yöntemde `source database` sadece kullanılacak olan tabloları airflow'a aktarmaktadır. Method1'deki gibi tranform işlemlerini gerçekteştirmemektedir.

![alt text](/images/method2_tasks.png)

> extract, truncate ve load isimleriyle başlayan bütün tasklar dinamik olarak oluşturulmaktadır.

1. `source database`'den `etl` süreci için gerekli olan bütün tablolar çekilir: `extract_*`
2. Çekilen veriler gerekli olan `transform task`'ına iletilir ve gerekli bütün işlemler gerçekleştirilir: `transform_*`
3. `transform` işleminden sonra oluşturulan verinin `load` edileceği tablo `truncate` edilir: `truncate_*`
4. `truncate` işleminden sonra load işlemi gerçekleştirilir: `load_*`

## Kurulum

Kurulum için sadece `Docker` gerekmektedir. `Docker` kurulu ise:

```bash
docker compose up airflow-init
docker compose up -d
```

> Docker kurulumu [için](https://docs.docker.com/engine/install/) <br/>
Docker ile airflow kurulumu ve daha fazla bilgi [için](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)


Birkaç dakika bekledikten sonra [buradaki](http://localhost:9080/) linke tıklayarak `airflow` arayüzüne gidebilirsiniz. 

> airflow `local`inizde `9080` portunda çalışmaktadır.

Açılan ekranda size `kullanıcı adı` ve `şifre` sorulacak. Kullanıcı adı ve şifre `airflow`'dur.

> İlk açılırken hata alıcaksınızdır. `Nasıl Çalıştırılır?` kısmını okumaya devam ederek hataları gidereceksiniz.


## Nasıl çalıştırılır?

Bu programda `task`'lar dinamik bir şekilde oluşturulmaktadır. Bu dinamiklik `airflow`'un `Variables` özelliği ile mümkün olmaktadır. Bağlantılara ait `connection id` değerleri, `extract` edilecek tabloların isimleri, `load` işlemi yapılacak edilecek tabloların isimleri `Variables` olarak airflow'a eklenir ve dinamiklik bu sayede gerçekleştirilir.

### Connection'ların eklenmesi

Bu program `source database` ve `target database` diye adlandırılan iki adet `PostgreSQL` veritabanı üzerinde işlem yapmaktadır. Bu veri tabanlarına bağlanmak için Airflow'un `Connections` kısmına database'lerin eklenmesi gerekmektedir.

> extract, transform ve load işlemlerinin hepsi pandas üzerinden gerçekleştirildiği için pandas'ın üzerinde işlem yapamadığı hiçbir şey bu programda gerçekleştirilemez.

Airflow'un `Admins` sekmesinde bulunan `Connections` sayfasını açtıktan sonra `+` işaretine basarak yeni bir connection ekleyelim.

Aşağıdaki gibi bir ekran gelmesi gerekmektedir. Bu ekranda gerekli yerleri doldurmalısınız.

![add_connection](images/add_connection.png)

* Connection Id: Bu değer connection için verdiğiniz bir isimdir. İstediğiniz ismi verebilirsiniz. Bağlantıya vereceğiniz isim `source database`'e bağlanırken kullanılacaktır. Bu örnek için `source_connection`

* Connection Type: Bir sürü bağlantı tipi bulunmaktadır. Biz PostgreSQL üzerinde işlem yapacağımız için Postgres'i seçin.

* Host: Database'in host'udur. Host'u öğrenmek için <br/> `docker inspect airflow-etl-processes-network -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}'` <br/> komutunu çalıştırınız. Çıkan değeri host kısmına yazınır.

* Schema: Schema olarak yazmaktadır lakin buradaki isim database olmaktadır. PostgreSQL içindeki üzerinde işlem yapacağımız database olmaktadır. BU örnek için `source` olarak yazınız.

* Login: Veritabanı bağlantısını sağlayacak olan username olmaktadır. `source` yazınız.

* Password: Veritabanı user'ının şifresinidir. `source` yazınız.

* Port: Veritabanı bağlantısı port'u. 8000 olarak giriniz.

En alt kısımdaki test butonundan bağlantının başarılı bir şekilde gerçekleştirip gerçekleştirilmediğini görebilirsiniz. Test başarılı ise (sonucunu en üste çıkarak görebilirsiniz) Save butonuna basınız.


Benzer şekilde target database'ini ekleyiniz. Target database için bilgiler:

* Connection Id : `target_connection`
* Connection Type: Postgres
* Host: `docker inspect airflow-etl-processes-network -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}'` komutunun çıktısı
* Schema: target
* Login: target
* Password: target
* Port: 8001

## Dummy verinin oluşturulması

Dummy veri oluşturmak için `dummy_data.py` script'ini çalıştırmanız yeterlidir. Aşağıdaki parametrelere dokunmasanıza gerek yoktur.

```
python3 dummy_data.py
```

### dummy_data.py parametreleri

```python
customer_count = 2500 # custormer_t tablosundaki satır sayısı. Müşteri sayısı.
store_count = 50 # store_t tablosundaki satır sayısı. Mağaza sayısı.
sales_count = 20000 # sales_tx_t, sales_t tablesundaki satır sayısı. Ürün satın alma sayısı

db_params = {
    "username": "source",
    "password": "source",
    "database": "source",
    "host": "localhost",
    "port": "8000"
}

schema = "public" # veri yazılacak database'in şemasının adı
```


## Method1

Birinci metot veritabanından query'ler ile `transform` edilmiş hazır veriyi alır ve `load` işleminin gerçekleştirileceği database'e veriyi yazar.

Bu metot'u çalıştırmak için `dags/method_1/configs.json` dosyasının airflow'a `import` edilmesi gerekmektedir.

`import` işlemi için `admins`'ın altında bulunan `variables` sayfasını açınız. Daha sonra sol taraftaki `Choose File` butonuna tıklayarak `dags/method_1/configs.json` dosyasını seçiniz ve `Import Variables` butonuna basınız. Bu işlemlerden sonra `DAGs` sayfasında `etl_method1` isimli bir DAG görmelisiniz.

> Eğer göremediyseniz biraz bekleyip sayfası yenileyin.

### config.json

Daha önce dinamik yapısın `Variables` ile sağlandığını ve `Variables` değerlerin ise `config.json` dosyasından geldiğini belirtmiştik. Şimdi JSON dosyasındaki yapının anlamını açıklayalım.

Method1 için JSON dosyasında 3 temel key değeri bulunmaktadır.

* method1_etl_configs: Method1 için gerekli olan belirli parametreleri barındırır. Load işleminin yapılacağı şemanın ve tablonun adı, `source database`'e çalışacak query ve load tablonun oluşturulması için load tablonun kolonları ve tip değerleri.

* source_connection: Connections kısmında bulunan Connection Id değeri burada bulunur. Bu variable sayesinde source database'ı değiştirmek için yapmanız gerek şey sadece connection id değerini değiştirmektedir.

* target_connection: Connections kısmında bulunan Connection Id değeri burada bulunur. Bu variable sayesinde target database'ı değiştirmek için yapmanız gerek şey sadece connection id değerini değiştirmektedir.

```json
{
    "method1_etl_configs" : {},
    "source_connection": "source_connection",
    "target_connection": "target_connection"
}
```


Aşağıda bir tane `method1_etl_configs` değerinin key ve value değerini görmektesiniz. Key değeri verinin yazılacak tablonun adını temsil etmektedir.

* query: method1 kaynak veritabanından hazır bir veriyi alıp pandas ile target database'e veriyi basmaktadır. Bu parametre veriyi hazırlayan query'nin kendisidir. query'i parametresi sonucu oluşsan tablo target database'e yazılır.

* write_schema: target database'deki verinin yazılacağı schema'nın adıdır.

* write_table: target database'deki verinin yazılacağı table'ın adıdır.

* columns: Eğer load edilecek tablo target database'e yoksa tabloyu oluşturmak için kolon isimlerini buraya yazıyoruz. Buraya create query'side yazılabilirdi niye böyle bir şey yaptım bilmiyorum :)

```json
"accounting_unit_data_mart": {
            "query": "select st.paid_price, CAST(st.create_date as DATE) from sales_tx_t stt left join sales_t st on stt.sales_id = st.id where stt.status = true;",
            "write_schema": "data_marts",
            "write_table": "accounting_unit_data_mart",
            "columns": "CREATE_DATE TIMESTAMP NOT null, PAID_PRICE NUMERIC(10, 2) NOT NULL"
}
```

### Method1'e yeni bir ekleme yapmak

Method1 için yeni bir load işlemi eklemek için yapmanız gereken sadece `method1_etl_configs` kısmına yeni bir ekleme yapmaktır.


## Method2

İkinci metot veritabanından verileri tablolar halinde çeker ve transform işlemini airflow'a bırakır.

Bu metot'u çalıştırmak için dags/method_2/configs.json dosyasının airflow'a import edilmesi gerekmektedir.

`import` işlemi için `admins`'ın altında bulunan `variables` sayfasını açınız. Daha sonra sol taraftaki `Choose File` butonuna tıklayarak `dags/method_1/configs.json` dosyasını seçiniz ve `Import Variables` butonuna basınız. Bu işlemlerden sonra `DAGs` sayfasında `etl_method1` isimli bir DAG görmelisiniz.

> Eğer göremediyseniz biraz bekleyip sayfası yenileyin.

### config.json

Daha önce dinamik yapısın `Variables` ile sağlandığını ve `Variables` değerlerin ise `config.json` dosyasından geldiğini belirtmiştik. Şimdi JSON dosyasındaki yapının anlamını açıklayalım.

```json
{
    "source_connection": "",
    "target_connection": "",
    "method2_extract_tables": "", 
    "method2_transformed_table_names":  "",
    "method2_transform_mapping": {}
}
```

* source_connection: Connections kısmında bulunan Connection Id değeri burada bulunur. Bu variable sayesinde source database'ı değiştirmek için yapmanız gerek şey sadece connection id değerini değiştirmektedir.

* target_connection: Connections kısmında bulunan Connection Id değeri burada bulunur. Bu variable sayesinde target database'ı değiştirmek için yapmanız gerek şey sadece connection id değerini değiştirmektedir.

* method2_extract_tables: extract edilecek tabloların isimleridir. Tabloların yazılma şekli: "{{schema}}.{{table}}, {{schema}}.{{table}}, ...." Örneğin: "public.sales_tx_t, public.sales_t"

* method2_transformed_table_names: Load edilecek tabloların isimlerdir. Tabloların yazılma şekli: "{{schema}}.{{table}}, {{schema}}.{{table}}, ...." Örneğin: "data_marts.accounting_unit_data_mart, data_marts.marketing_unit_data_mart"

* method2_transform_mapping: transform işleminde kullanılacak verilerin kullanıcılağı extract edilmiş tablolar. key'in yazılma şekli {{schema}}{{table}}. Value'nin ise "{{schema}}.{{table}}, {{schema}}.{{table}}, ...."


### Method2'e yeni bir ekleme yapmak

* Eğer yeni tablo kullanılacaksa `method2_extract_tables` kısmına yeni tabloyu ekleyin.
* Yeni load edilecek tablonun adını `method2_transformed_table_names`'e ekleyin.
* load edilecek yeni tablonun oluşturulması için gereken tablolar buraya eklenir.