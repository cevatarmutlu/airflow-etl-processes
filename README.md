## airflow-etl-processes

Airflow kullanılarak iki farklı `etl` süreci içeren bir repodur. Bu `etl` süreçleri olabildiğince dinamik bir yapıda yapılmıştır.

* Birinci yöntemde `extract` ve `transform` işlemlerindeki yük verinin çekildiği `source database`'e bırakılmıştır. Database'den dönüştürülmüş şekilde elde edilen veriler `target database`'e yüklenmiştir.

* İkinci yöntemde `transaction` işlemlerinin gerçekleştirildiği bir veri tabanına `transform` gibi maliyetli bir aşamayı yaptırmak `source database`'in başka isteklere cevap vermesini yavaşlatacağı için `source database`'e sadece `extract` işlemleri yaptırılmıştır. `transform` ve `load` işlemleri `source database`'den ayrı olarak yapılmıştır.

> Bütün ETL sürecindeki işlemler `pandas` kullanılarak yapılmıştır.

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


## Nasıl çalışır?

Bu kısımda kullanılan `etl` süreçlerine dair detaylı bir anlatım yapılacaktır.

### Method 1

Bu yöntemde, transform işlemlerinin oluşturduğu yük veritabanına verilmiştir. Aşağıda `method1` sürecinin `task` akışını görmektesiniz.

> `_data_mart` ifadelerinin bulunduğu kısımlar dinamik olarak kod ile oluşturulmaktadır.

![alt text](/./method1_tasks.png)


* `truncate_or_create`: Load işleminin yapılacağı tablo bir `truncate` edilmektedir. Eğer bu tablo `target database`de yoksa tablo için `create` işlemi yapılmaktadır.
* `extract_transform`: `transform` işlemi için gereken filtreleme, join gibi işlemleri barındıran bir `SQL Query`'si ile `source database`'den `transform` edilmiş veri elde edilir.
* `load`: `extract_transform` task'ından elde edilen veriyi hiç dokunmadan `target database`'e load eden task.


#### Method1'in eksikleri ya da bağımlılıkları

1. Bu method `prod` ortamdaki bir database'den veri çekerken prod'daki database'in diğer client'lara cevap verme süresini önemli ölçüde yavaşlatabilir çünkü filtreleme ve join gibi işlemler database tarafından yapılmaktadır.
2. Bu yöntemde `source database` dışında gelen bir datanın işleme tabi olması mümkün değildir.


## Method2

Bu yöntemde `source database` sadece kullanılacak olan tabloları airflow'a aktarmaktadır. Method1'deki gibi tranform işlemlerini gerçekteştirmemektedir.

![alt text](/./method2_tasks.png)

> extract, truncate ve load isimleriyle başlayan bütün tasklar dinamik olarak oluşturulmaktadır.

1. `source database`'den `etl` süreci için gerekli olan bütün tablolar çekilir.
2. Çekilen veriler gerekli olan `transform task`'ına iletilir ve gerekli bütün işlemler gerçekleştirilir. 
3. `transform` işleminden sonra oluşturulan verinin `load` edileceği tablo `truncate` edilir. 
4. `truncate` işleminden sonra load işlemi gerçekleştirilir.