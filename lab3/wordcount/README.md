# Lab3 Wordcount mit ZeroMQ

Diese Loesung bildet die Aufgabe aus `lab3/README.md` mit dem Parallel-Pipeline-Muster ab:

- 1 Splitter
- 3 Mapper
- 2 Reducer

Der Splitter verteilt Saetze per `PUSH` an die Mapper. Jeder Mapper zerlegt Saetze in Woerter und sendet jedes Wort an genau einen Reducer. Die Zuordnung ist stabil, damit gleiche Woerter immer beim selben Reducer landen. Jeder Reducer zaehlt seine Woerter und gibt nach jeder Aktualisierung den aktuellen Stand aus.

## Starten

Alle Befehle werden aus diesem Ordner gestartet:

```powershell
cd C:\dev\git\vs2lab\lab3\wordcount
```

Terminal 1:

```powershell
pipenv run python reducer.py 0
```

Terminal 2:

```powershell
pipenv run python reducer.py 1
```

Terminal 3:

```powershell
pipenv run python mapper.py 1
```

Terminal 4:

```powershell
pipenv run python mapper.py 2
```

Terminal 5:

```powershell
pipenv run python mapper.py 3
```

Terminal 6:

```powershell
pipenv run python splitter.py
```

Optional kann der Splitter eine andere Textdatei lesen:

```powershell
pipenv run python splitter.py meine_datei.txt
```

