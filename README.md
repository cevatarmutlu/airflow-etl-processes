## airflow-etl-processes

Airflow kullanılarak iki farklı `etl` süreci içeren bir repodur. Bu `etl` süreçleri olabildiğince dinamik bir yapıda yapılmıştır.

* Birinci yöntemde `extract` ve `transform` işlemlerindeki yük verinin çekildiği `source database`'e bırakılmıştır. Database'den dönüştürülmüş şekilde elde edilen veriler `target database`'e yüklenmiştir.

* İkinci yöntemde `transaction` işlemlerinin gerçekleştirildiği bir veri tabanına `transform` gibi maliyetli bir aşamayı yaptırmak `source database`'in başka isteklere cevap vermesini yavaşlatacağı için `source database`'e sadece `extract` işlemleri yaptırılmıştır. `transform` ve `load` işlemleri `source database`'den ayrı olarak yapılmıştır.

## Kurulum

Kurulum için sadece `Docker` gerekmektedir. `Docker` kurulu ise:

```bash
docker compose up airflow-init
docker compose up -d
```

> Docker üzerinden airflow kurulumu ve daha fazla bilgi [için](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)


Birkaç dakika bekledikten sonra [buradaki](http://localhost:9080/) linke tıklayarak `airflow` arayüzüne gidebilirsiniz. 

> airflow `local`inizde `9080` portunda çalışmaktadır.

Açılan ekranda size kullanıcı adı ve şifre sorulacak. Kullanıcı adı ve şifre `airflow`'dur.

