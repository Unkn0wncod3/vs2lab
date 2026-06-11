# Lab 3 Antworten

Hier sind die Antworten zu den Aufgaben aus `lab3/README.md` und ganz kurz die Erklärung zur eigenen Wordcount-Lösung.

## Lab3.1 Request-Reply

### Experiment 1

Hier wird zuerst der Client und danach der Server gestartet. Der Client kann die Anfrage schon losschicken, obwohl der Server noch nicht läuft. Er wartet dann bei `recv()`, bis eine Antwort kommt. Sobald der Server startet, verarbeitet er die Anfrage und schickt die Antwort zurück. Das zeigt gut, dass ZeroMQ asynchron arbeitet.

### Experiment 2

Hier werden zwei Clients vor dem Server gestartet. Der Server bindet an zwei Adressen und kann deshalb mit beiden Clients sprechen. Beide Clients senden ihre Anfragen an denselben Server und bekommen jeweils Antworten zurück. Der Server endet erst, wenn `client.py` die Nachricht `STOP` sendet.

## Lab3.2 Publish-Subscribe

### Experiment 1

Beide Clients abonnieren das Thema `TIME`. Deshalb bekommen auch beide dieselben Zeit-Nachrichten. Die `DATE`-Nachrichten interessieren sie nicht und werden ignoriert.

### Experiment 2

Jetzt abonnieren die Clients unterschiedliche Themen. `client.py` hört auf `TIME`, `client1.py` auf `DATE`. Deshalb bekommen sie auch unterschiedliche Nachrichten. Das zeigt gut, dass beim Publish-Subscribe-Muster nach Themen gefiltert wird.

## Lab3.3 Parallel Pipeline

### Experiment 1

Hier gibt es zwei Farmer, aber nur einen Worker. Deshalb landet die ganze Arbeit bei diesem einen Worker. Er bekommt Aufgaben von beiden Farmern und verarbeitet alles nacheinander.

### Experiment 2

Hier gibt es einen Farmer und zwei Worker. Die Aufgaben werden auf beide Worker verteilt. Dadurch teilen sich die Worker die Arbeit, was genau die Idee der Pipeline ist.

## Eigene Wordcount-Lösung

Meine Lösung besteht aus:

- einem `splitter.py`
- drei `mapper.py`
- zwei `reducer.py`

Der Splitter liest die Datei zeilenweise ein und verteilt die Sätze an die Mapper. Die Mapper zerlegen die Sätze in Wörter und schicken jedes Wort an genau einen passenden Reducer. Die Reducer zählen dann mit, wie oft jedes Wort vorkommt.

Wichtig ist dabei:

- Die Sätze werden auf die Mapper verteilt.
- Gleiche Wörter landen immer beim selben Reducer.
- Jeder Reducer zählt nur seinen Teil.
- Am Ende ergeben beide Reducer zusammen das Gesamtergebnis.

## Start

Im Ordner `lab3/wordcount`:

1. `pipenv run python reducer.py 0`
2. `pipenv run python reducer.py 1`
3. `pipenv run python mapper.py 1`
4. `pipenv run python mapper.py 2`
5. `pipenv run python mapper.py 3`
6. `pipenv run python splitter.py`

Optional mit anderer Datei:

`pipenv run python splitter.py meine_datei.txt`
