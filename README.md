## airflow-etl-processes

Airflow kullanılarak iki farklı `etl` süreci içeren bir repodur. Bu `etl` süreçleri olabildiğin dinamik bir yapıda yapılmıştır.

* Birinci yöntemde `extract` ve `transform` işlemlerindeki yük verinin çekildiği `source database`'e bırakılmıştır. Database'den dönüştürülmüş şekilde elde edilen veriler `target database`'e yüklenmiştir.

* İkinci yöntemde `transaction` işlemlerinin gerçekleştirildiği bir veri tabanına `transform` gibi maliyetli bir aşamayı yaptırmak `source database`'in başka isteklere cevap vermesini yavaşlatacağı için `source database`'e sadece `extract` işlemleri yaptırılmıştır. `transform` ve `load` işlemleri `source database`'den ayrı olarak yapılmıştır.

## Kurulum

### Dummy Data


