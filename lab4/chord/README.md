# Lab 4 Chord

Diese Implementierung baut einen vereinfachten Chord-Ring auf. Jeder
Chord-Knoten läuft als eigener Prozess und kommuniziert über
`lab_channel`/Redis mit den anderen Prozessen.

## Peer-Implementierung

Die Peer-Implementierung ist die Klasse `ChordNode` in `chordnode.py`.

Ein Objekt dieser Klasse ist ein Peer im Chord-Ring. Es registriert sich in der
Channel-Gruppe `node`, kennt andere Knoten, berechnet seine Finger Table und
bearbeitet eingehende Nachrichten in `run()`.

Die Peers werden in `doit.py` gestartet:

```python
mp.Process(
    target=create_and_run,
    name="ChordNode-" + str(i),
    args=(m, chord_node.ChordNode, bar1, bar2))
```

Bei `n = 8` werden acht `ChordNode`-Prozesse gestartet. Der `DummyChordClient`
ist kein Peer im Ring, sondern nur ein Client, der eine Lookup-Anfrage an einen
zufälligen Peer stellt.

## Rekursive Namensaufloesung

Ein `LOOKUP` gibt direkt den zuständigen Knoten `succ(key)` zurück. Dafür
wurde in `ChordNode` die Methode `recursive_successor_node()` ergänzt.

Der Ablauf ist:

1. Der Knoten bestimmt mit `local_successor_node(key)` den besten bekannten
   nächsten Knoten aus seiner Finger Table.
2. Wenn dieser Knoten er selbst ist, ist die Suche fertig.
3. Sonst sendet er eine neü `LOOKUP_REQ` an den nächsten Knoten.
4. Er wartet auf dessen `LOOKUP_REP`.
5. Das Ergebnis wird an den vorherigen Sender zurückgegeben.

Damit fragt nicht der Client alle Knoten nacheinander ab. Stattdessen setzen die
Peers die Suche rekursiv untereinander fort.

## Client

Der `DummyChordClient` in `doit.py` wurde so erweitert, dass er:

1. alle existierenden Chord-Knoten aus der Gruppe `node` liest,
2. einen zufälligen validen Key aus dem Namensraum wählt,
3. einen zufälligen existierenden Startknoten wählt,
4. eine `LOOKUP_REQ` sendet,
5. das gefundene `succ(key)` ausgibt,
6. danach alle Knoten mit `STOP` beendet.

## Ausführen

Redis muss laufen. Danach:

```powershell
cd C:\dev\git\vs2lab
pipenv run python lab4\chord\doit.py
```
