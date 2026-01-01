# Vertigo Games Data Case - Part 2

Bu proje, Vertigo Games vaka çalışmasının 2. bölümü için hazırlanmıştır. Amaç, ham oyun verilerini temizleyip anlamlı iş metriklerine dönüştürmek ve analiz etmektir.

## 📂 İçerik
* **`daily_metrics.sql`**: Ham veriyi (events) temizleyen ve günlük KPI'ları hesaplayan SQL kodu.
* **v-p2.pdf**: Looker Studio ile hazırlanan basit dashboard.

## ⚙️ Yaklaşım 

Veriler BigQuery'e yüklendi. Basit bir sql ile istenen formatta bir tabloya basıldı. 

Looker studio ile BigQuery'den çekilen veri ile "çok basit" bir dashboard oluşturuldu.

Herhangi bir BI tool'u daha önce hiç kullanmamıştım dolayısıyla "çok basit", aynı zamanda çok konforsuz, bir çalışma oldu. 

Temel olarak gelirlerin çoğunun(%95) oyun içi alımlardan olduğunu, kullanıcı sayısı ve gelirler dengesinde bu konuya Amerika üzerinden dikkat çekmek istedim.

---
*Ahmet Hakan Ekşi*
