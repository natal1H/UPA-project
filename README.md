# UPA - Covid-19

### Členovia tímu

- xbalif00
- xholko02
- xzitny01
<br/><br/>
 
***

## Výbrané dotazy a potrebné data

Pre nasledujúcu časť projektu sme si vybrali tieto dotazy:

- ***Dotazy skupiny A:*** 
```
A1

Vytvořte čárový (spojnicový) graf zobrazující vývoj covidové situace po měsících pomocí následujících hodnot: 
počet nově nakažených za měsíc, počet nově vyléčených za měsíc, počet nově hospitalizovaných osob za měsíc, počet provedených testů za měsíc. 
Pokud nebude výsledný graf dobře čitelný, zvažte logaritmické měřítko, nebo rozdělte hodnoty do více grafů. 
```

Pre bod ***A1*** je nutné získať počet *nakazených*, *vyliečených* a *hospitalizovaných* osôb za mesiac a počet *vykonaných testov* za mesiac.

```
A3

Vytvořte sérii sloupcových grafů, které zobrazí:

1. graf: počty provedených očkování v jednotlivých krajích (celkový počet od začátku očkování).
2. graf: počty provedených očkování jako v předchozím bodě navíc rozdělené podle pohlaví. Diagram může mít např. dvě části pro jednotlivá pohlaví.
3. graf: Počty provedených očkování, ještě dále rozdělené dle věkové skupiny. Pro potřeby tohoto diagramu postačí 3 věkové skupiny (0-24 let, 25-59, nad 59).
```

V bode ***A3*** sa musíme oboznámiť s jednotlivými *krajami*, ich priradenými *kódmi* a následne s počtami *očkovaní* v jednotlivých krajoch a ich závislosťami na *pohlavie* a *vekové skupiny*.

- ***Dotazy skupin B:***

```
B1

Sestavte 4 žebříčky krajů "best in covid" za poslední 4 čtvrtletí (1 čtvrtletí = 1 žebříček). 
Jako kritérium volte počet nově nakažených přepočtený na jednoho obyvatele kraje. Pro jedno čtvrtletí zobrazte výsledky také graficky. 
Graf bude pro každý kraj zobrazovat celkový počet nově nakažených, celkový počet obyvatel a počet nakažených na jednoho obyvatele. 
Graf můžete zhotovit kombinací dvou grafů do jednoho (jeden sloupcový graf zobrazí první dvě hodnoty a druhý, čárový graf, hodnotu třetí). 
```

V bode ***B1*** sa zameriame znova na *kraje*, počet ich *obyvateľov* a následne počty *nakazených* v prepočte na jedného obyvateľa daných krajov. 

- ***Dotazy skupiny C:***

```
C1

Hledání skupin podobných měst z hlediska vývoje covidu a věkového složení obyvatel.

Atributy: počet nakažených za poslední 4 čtvrtletí, počet očkovaných za poslední 4 čtvrtletí, počet obyvatel 
ve věkové skupině 0..14 let, počet obyvatel ve věkové skupině 15 - 59, počet obyvatel nad 59 let.
Pro potřeby projektu vyberte libovolně 50 měst, pro které najdete potřebné hodnoty (můžete např. využít nějaký žebříček 50 nejlidnatějších měst v ČR).
```

Vo finálnom dotaze ***C1*** je nutné získať data o jednotlivých *mestách* a *veku* ich obyvateľov, kedy budeme sledovať počet *nakazených* a *očkovaných*. 



### Popis zdrojov

***Nakazení:*** dátum ***|*** vek ***|*** pohlavie ***|*** kraj_kód ***|*** okres_kód ***|*** zahraničie ***|*** zem_zahraničie   

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.csv
<br/><br/>

***Vyliečení:*** dátum ***|*** vek ***|*** pohlavie ***|*** kraj_kód ***|*** okres_kód 

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/vyleceni.csv
<br/><br/>

***Hospitalizovaní:*** dátum ***|*** prvý_záznam ***|*** kum_prvý_záznam ***|*** počet ***|*** bez priznakov ***|*** ... 

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.csv
<br/><br/>

***Testy:*** dátum ***|*** pocet_PCR ***|*** pocet_AG ***|*** typológie ***|*** pozitívny ***|*** ...

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy-pcr-antigenni.csv
<br/><br/>


***Očkovanie(regiony):*** dátum ***|*** vakcína ***|*** kraj_kód ***|*** kraj_názov ***|*** veková_skupina ***|*** typy_dávok ***|*** celkom_dávok  

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.csv
<br/><br/>

***Očkovanie(ľudia):*** dátum ***|*** vakcína ***|*** kód ***|*** poradie ***|*** veková_skupina ***|*** pohlavie ***|*** pocet_dávok

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-demografie.csv
<br/><br/>

***Mŕtvi:*** dátum ***|*** vek ***|*** pohlavie ***|*** kraj_kód ***|*** okres_kód

⬇ https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/umrti.csv
<br/><br/>

***

## Schéma dat a ich zpracovanie

***Nakazení:*** výsledné data ohľadne nakazených osob bolo nutné očistiť od 
nepotrebných informácií a previesť pohlavie podľa slovníka.


| date | age | gender | region | district |
|------|-----|--------|--------|----------|

***Vyliečení:*** tieto data zostali v pôvodnej podobe s prevedením pohlavia podľa slovníka.

| date | age | gender | region | district |
|------|-----|--------|--------|----------|

***Hospitalizovaní:*** v dátach o hospitalizovaných nám postačujú 
iba informácie o dátume a počte hospitalizovaných. Ostatné boli odstránené a hodnota dátumu upravená pre naše potreby.
Počty hospitalizovaných osôb sú zoskupené podľa mesiacov.

| month | patients |
|-------|----------|

***Testy:*** pri počte vykonaných testov sa ponechali iba dátum a počet PCR a AG testov, ktoré sa následne zoskupili do jednej hodnoty vyjadrujúcej počet celkových testov za jednotlivé mesiace.

| month | tests |
|-------|-------|

***Očkovanie(regióny):*** pre získanie počtu očkovaní v jednotlivých krajoch sme museli vyčitiť data od nepotrebných informácií
a následne ich zoskupiť podla krajov.

| region | count |
|--------|-------|

***Očkovanie(ľudia):*** pre pozorovanie počtu očkovaných ľudí vzhľadom na pohlavie a vekovú skupinu, bolo nutné
získať data zo spomenutého druhého zdroja a jemne očistiť data.

| date | age_group | gender |
|------|-----------|--------|

***Mŕtvi:*** data o úmrtiach budú ďalej vhodné pri odpovedaní na vlastné dotazy.

| date | age | gender | region | district |
|------|-----|--------|--------|----------|

***

## Vybrané technológie a spustenie

### Technologie:

- ***Python3.8***
- ***MongoDB*** - NoSQL DB
- ***Pandas*** - manipulácia a analýza dat

### Inštalácia:

Inštalácia MongoDB - https://docs.mongodb.com/manual/administration/install-community/

```
python3 -m venv ./env-upa
source ./env-upa/bin/activate
pip3 install -r requirements.txt
```
### Spustenie:


```
Príklad spustenia:
python3 data_loader.py --mongo mongodb://localhost:27017/ -f download_data_folder -d DB
```