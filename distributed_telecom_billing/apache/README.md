# Apache HTTP Server Setup

## Option A - XAMPP (easiest)
1. Install XAMPP from https://www.apachefriends.org
2. Replace `xampp/apache/conf/httpd.conf` with the httpd.conf in this folder.
   Edit the Alias line so it points at your real path to
   distributed_telecom_billing/powerbi/dashboard_data/
3. Start **Apache** in the XAMPP control panel.
4. Verify: http://127.0.0.1:80/health  (or just http://127.0.0.1/health)

## Option B - Standalone Apache (Ubuntu)
    sudo apt install apache2
    sudo cp httpd.conf /etc/apache2/sites-available/telecom.conf
    sudo a2enmod proxy proxy_http headers rewrite
    sudo a2ensite telecom
    sudo systemctl reload apache2

## Verify
    curl http://127.0.0.1:80/health
    curl http://127.0.0.1:80/api/stats
    curl http://127.0.0.1:80/data/transactions.csv
