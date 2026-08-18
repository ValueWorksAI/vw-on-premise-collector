# ValueWorks On-Premise Collector — Vorbereitung für die IT

Diese Seite beschreibt, was vor dem Einrichtungstermin bereitstehen sollte. Wenn die
Punkte unter „Vor dem Termin" erledigt sind, dauert die eigentliche Einrichtung
ca. 30 Minuten.

## Was der Collector macht

Ein kleines Python-Programm läuft einmal täglich auf einem Rechner in Ihrem Netz,
liest **lesend** aus den vereinbarten Datenbanken/Schnittstellen, schreibt das
Ergebnis als Parquet-Dateien in ein lokales Ausgabeverzeichnis und lädt diese per
HTTPS in den Azure-Blob-Storage von ValueWorks. Danach werden die lokalen Dateien
wieder aufgeräumt.

**Was ausdrücklich *nicht* nötig ist:**

- Kein eingehender Zugriff in Ihr Netz. Der Datenfluss ist ausschließlich ausgehend.
- Kein VPN, keine Portweiterleitung, keine feste IP auf Ihrer Seite.
- Kein Schreibzugriff auf die Quelldatenbanken — ein reines Leserecht genügt.
- Keine Domain-Admin-Rechte. Lokale Adminrechte werden nur einmalig für die
  Installation von Python/AzCopy benötigt.

Einziges Firewall-Ziel: `https://<konto>.blob.core.windows.net` (TCP 443).
Den konkreten Kontonamen erhalten Sie von ValueWorks.

## Vor dem Termin

### 1. Rechner für den Collector

Bitte **kein** Arbeitsplatz-PC, der abends ausgeschaltet oder in den Standby geht —
sonst läuft der geplante Task nicht. Ideal ist eine kleine, dauerhaft laufende
Windows-VM oder ein Server.

- Windows Server oder Windows 10/11, dauerhaft eingeschaltet
- **mindestens 16 GB RAM.** Der Collector hält eine Tabelle beim Verarbeiten
  vollständig im Arbeitsspeicher, bevor sie geschrieben wird — der Bedarf richtet sich
  also nach der größten einzelnen Tabelle, nicht nach der Gesamtdatenmenge.
- mindestens 20 GB freier Plattenplatz für Zwischendateien (die Dateien werden nach
  jedem Lauf wieder gelöscht)
- Netzwerkzugriff auf die Quell-Datenbankserver
- Ein Dienstkonto für den geplanten Task, dessen Passwort **nicht abläuft**

### 2. Lesender Datenbank-Benutzer

Der Collector verwendet **SQL-Server-Authentifizierung** (Benutzername + Passwort),
nicht Windows-Authentifizierung. Falls die Instanz auf „Windows-Authentifizierung
only" steht, bitte vorab melden — dann brauchen wir eine andere Lösung.

```sql
-- Auf der SQL-Instanz ausführen (Passwort bitte ersetzen):
USE [master];
CREATE LOGIN [vw_readonly] WITH PASSWORD = 'BITTE-ERSETZEN', CHECK_POLICY = ON;

USE [<DATENBANKNAME>];
CREATE USER [vw_readonly] FOR LOGIN [vw_readonly];
ALTER ROLE [db_datareader] ADD MEMBER [vw_readonly];
```

Bitte pro Quelldatenbank ausführen. Zusätzlich prüfen:

- TCP/IP-Protokoll ist für die Instanz aktiviert (SQL Server Configuration Manager)
- Der Port (Standard 1433, bei benannten Instanzen abweichend) ist vom
  Collector-Rechner erreichbar

Wir brauchen von Ihnen: **Servername\Instanz, Port, Datenbankname, Benutzer, Passwort.**
Das Passwort bitte über einen sicheren Kanal, nicht per Klartext-Mail.

### 3. Ausgehender Internetzugang

- TCP 443 vom Collector-Rechner nach `*.blob.core.windows.net` freigeben
- Falls ein **HTTP(S)-Proxy** im Einsatz ist: Adresse, Port und ob eine
  Authentifizierung nötig ist
- Falls der Proxy **TLS aufbricht** (SSL-Inspection): bitte vorab melden. Das ist
  die häufigste Ursache für fehlschlagende Uploads und wir müssen es dann gezielt
  konfigurieren.

### 4. Software (lokale Adminrechte nötig)

| Software | Bezugsquelle | Hinweis |
|---|---|---|
| Python 3.10–3.14 | <https://www.python.org/downloads/> | **Nicht** aus dem Microsoft Store — die Store-Variante funktioniert unter Dienstkonten nicht zuverlässig. Bei der Installation „Add python.exe to PATH" anhaken. |
| Poetry | `pip install poetry` | |
| AzCopy v10 | <https://aka.ms/downloadazcopy-v10-windows> | ZIP entpacken, `azcopy.exe` nach `C:\AzCopy\` legen |

### 5. Bei unklarem Datenbank-Backend

Wenn bei einer der Anwendungen nicht bekannt ist, auf welcher Datenbank sie läuft:
Bitte auf dem betreffenden Server einmal die installierten ODBC-Treiber auflisten.
Ohne installiertes Python geht das auch über *ODBC-Datenquellen (64-Bit)* →
Reiter *Treiber*, oder per PowerShell:

```powershell
Get-OdbcDriver | Select-Object Name, Platform
```

Ein Screenshot oder die Textausgabe genügt. Damit klären wir vor dem Termin, ob wir
per SQL Server, per generischem ODBC oder per Datei-Export anbinden müssen.

## Einrichtung ohne gemeinsamen Termin

Wenn Sie die Einrichtung selbst durchführen, lässt sich alles außer dem letzten Schritt
ohne uns erledigen. Zunächst die Schritte 1–6 oben, dann:

```powershell
poetry run python tools\check_azure.py --container warehouse --prefix raw/<name>
poetry run python tools\probe_mssql.py --drivers
poetry run python tools\probe_mssql.py --host <server> --database <db> `
    --user vw_readonly --sample --out <name>-schema.json
```

Bitte senden Sie uns zurück:

1. Die Ausgabe von `check_azure.py` (enthält keine Zugangsdaten — die Signatur des Tokens
   wird maskiert), damit wir die Storage-Verbindung bestätigen können
2. Die Ausgabe von `probe_mssql.py --drivers`
3. Die erzeugte Datei `<name>-schema.json`

Daraus erstellen wir die Konfigurationsdatei, die festlegt, welche Tabellen abgeholt
werden. **Bis Sie diese Datei von uns erhalten haben, hat der Collector nichts
abzuarbeiten** — ein Lauf endet dann mit „No sources to run". Das ist zu diesem Zeitpunkt
korrekt und kein Fehler. Den geplanten Task (Schritt 8) legen Sie am besten erst an,
nachdem die Konfigurationsdatei vorliegt.

Falls ein Schritt fehlschlägt, ist die Logausgabe für uns hilfreicher als eine
Beschreibung — bitte möglichst als Text und nicht als Screenshot.

## Ablauf des Termins (ca. 30–45 Min)

1. Verbindungstest zum Azure-Storage (`tools/check_azure.py`) — zuerst, weil hier
   Proxy- und Firewall-Themen sichtbar werden
2. Verbindungstest zur Datenbank und Schema-Auslesen (`tools/probe_mssql.py`)
3. Collector installieren, `.env` mit den Zugangsdaten füllen
4. Geplanten Task anlegen (täglich, nachts)

Die Auswahl der konkreten Tabellen und Felder passiert **nach** dem Termin bei
ValueWorks auf Basis des Schema-Dumps. Dafür ist kein weiterer Termin mit Ihnen
nötig; es kommt lediglich eine kleine Konfigurationsdatei zum Ablegen.

## Datenschutz und Nachvollziehbarkeit

- Zugangsdaten liegen ausschließlich lokal in der Datei `.env` auf dem
  Collector-Rechner, nicht im Quellcode und nicht bei ValueWorks.
- Der Zugriff auf den Azure-Storage erfolgt über ein zeitlich begrenztes,
  jederzeit widerrufbares SAS-Token — beschränkt auf genau einen Container,
  ohne Löschrecht.
- Jeder Lauf schreibt ein Logfile nach `C:\logs\vw-on-prem-collector`.
- Der Collector kann jederzeit gestoppt werden, indem der geplante Task deaktiviert
  wird. Es bleiben keine Dienste oder Hintergrundprozesse zurück.
