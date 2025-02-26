# Spesialisthelsetjenesten

Her finnes programmer som brukes i produksjonssløp for statistikkene som inngår i [Spesialisthelsetjenesten](https://www.ssb.no/helse/helsetjenester/statistikk/spesialisthelsetjenesten). 

Spesialisthelsetjensten er inndelt i følgende statistikkområder:
+ [Spesialisthelsetjenesten - Regnskap](https://github.com/statisticsnorway/stat-speshelse-regnskap)
+ [Spesialisthelsetjenesten - Personell](https://github.com/statisticsnorway/stat-speshelse-personell)
+ [Spesialisthelsetjenesten - Aktivitet og tjenester](https://github.com/statisticsnorway/stat-speshelse-aktivitet)

I tillegg ligger programmer relatert til geografiske analyser (GIS) her:
+ [Spesialisthelsetjenesten - Geografiske analyser (GIS)](https://github.com/statisticsnorway/stat-speshelse-gis)

## Årshjul

I dette repoet ligger hovedsakelig programmer som er felles for alle produksjonsløpene i statistikkområdene. Disse oppgavene er følgende:
+ [Dropplister](https://github.com/statisticsnorway/speshelse/blob/master/experimental/Droplister%20forenkling.ipynb)
+ [Institusjonslister](https://github.com/statisticsnorway/speshelse/blob/master/Institusjonslister/Institusjonslister.R)
+ [Overføring av skjemadata til Samdata](https://github.com/statisticsnorway/speshelse/tree/master/Samdata)

<img src="./images/Årshjul.PNG" width="1200">


## KLASS

Før noen av produksjonsløpene kan kjøres må alle kodelistene som omfatter spesialisthelsetjenesten være oppdatert for statistikkåret. 

### Felles
+ `KLASS 603`: [Standard for offentlige helseforetak](https://www.ssb.no/klass/klassifikasjoner/603)
+ `KLASS 604`: [Standard for private helseinstitusjoner med oppdrags- og bestillerdokument](https://www.ssb.no/klass/klassifikasjoner/604)
+ `KLASS 605`: [Kodeliste for regionale og felleseide støtteforetak i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/605)
+ `KLASS 610`: [Kodeliste for tjenesteområder i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/610)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 6`](https://www.ssb.no/klass/klassifikasjoner/610/korrespondanser/898)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 603`](https://www.ssb.no/klass/klassifikasjoner/603/korrespondanser/1320)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 604`](https://www.ssb.no/klass/klassifikasjoner/604/versjon/1721/korrespondanser/1262)
+ Korrespondansetabell mellom `KLASS 610` og `KLASS 605`

### Regnskap
+ `KLASS 602`: [Kodeliste for funksjonskontoinndeling til helseforetakenes regnskapsdata](https://www.ssb.no/klass/klassifikasjoner/602/)
+ `KLASS 606`: [Kodeliste for artskontoinndeling til helseforetakenes regnskapsdata](https://www.ssb.no/klass/klassifikasjoner/606)
+ `KLASS 653`: [Kodeliste for artskontoinndeling for private helseinstitusjoners regnskapsdata](https://www.ssb.no/klass/klassifikasjoner/653)
+ `KLASS 652`: [Kodeliste for inntekter og kostnader i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/652)
+ `KLASS 649`: [Kodeliste for tjenesteområder innenfor regnskap i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/649/)
+ `KLASS 687`: [Kodeliste for artskontoinndeling til helseforetakenes balanseregnskap](https://www.ssb.no/klass/klassifikasjoner/687)
+ [Korrespondansetabell mellom `KLASS 602` og `KLASS 610`](https://www.ssb.no/klass/klassifikasjoner/602/korrespondanser/1575)
+ [Korrespondansetabell mellom `KLASS 606` og `KLASS 652`](https://www.ssb.no/klass/klassifikasjoner/606/korrespondanser/1535)
+ [Korrespondansetabell mellom `KLASS 653` og `KLASS 652`](https://www.ssb.no/klass/klassifikasjoner/653/korrespondanser/1408)
+ [Korrespondansetabell mellom `KLASS 602` og `KLASS 649`](https://www.ssb.no/klass/klassifikasjoner/649/korrespondanser/1575)

### Aktivitet
+ `KLASS 612`: [Kodeliste for døgnplasser og sengedøgn](https://www.ssb.no/klass/klassifikasjoner/612/)
+ [Korrespondansetabell mellom `KLASS 612` og `KLASS 610`](https://www.ssb.no/klass/klassifikasjoner/612/korrespondanser/893)

### Personell
+ `KLASS 628`: [Kodeliste for yrkesgrupper i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/628)
+ `KLASS 639`: [Kodeliste for utdanningsgrupper i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/639)
+ `KLASS 647`: [Kodeliste for pasient- og brukerrettede stillinger](https://www.ssb.no/klass/klassifikasjoner/647)
+ `KLASS 676`: [Kodeliste for spesialiteter for avtalespesialistene (innsamling)](https://www.ssb.no/klass/klassifikasjoner/676)
+ `KLASS 677`: [Kodeliste for spesialiteter for avtalespesialistene (publisert)](https://www.ssb.no/klass/klassifikasjoner/677)
+ [Korrespondansetabell mellom `KLASS 628` og `KLASS 7`](https://www.ssb.no/klass/klassifikasjoner/628/korrespondanser/1007)
+ [Korrespondansetabell mellom `KLASS 639` og `KLASS 207`](https://www.ssb.no/klass/klassifikasjoner/639/korrespondanser/1125)
+ [Korrespondansetabell mellom `KLASS 647` og `KLASS 7`](https://www.ssb.no/klass/klassifikasjoner/647/korrespondanser/1250)
+ [Korrespondansetabell mellom `KLASS 676` og `KLASS 208`](https://www.ssb.no/klass/klassifikasjoner/676/korrespondanser/1484)

### Avstand til fødested
+ `KLASS 608`: [Kodeliste for fødeavdelinger](https://www.ssb.no/klass/klassifikasjoner/608)
+ `KLASS 609`: [Kodeliste for fødestuer](https://www.ssb.no/klass/klassifikasjoner/609)

### Avstand til akuttmottak
+ `KLASS 683`: [Kodeliste for akuttmottak](https://www.ssb.no/klass/klassifikasjoner/683)

### Opptaksområder
+ `KLASS 629`: [Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)](https://www.ssb.no/klass/klassifikasjoner/629)
+ `KLASS 630`: [Kodeliste for opptaksområder i spesialisthelsetjenesten (psykisk helsevern)](https://www.ssb.no/klass/klassifikasjoner/630)
+ `KLASS 631`: [Kodeliste for opptaksområder i spesialisthelsetjenesten (rusbehandling)](https://www.ssb.no/klass/klassifikasjoner/631)
+ `KLASS 632`: [Kodeliste for opptaksområder i spesialisthelsetjenesten (DPS)](https://www.ssb.no/klass/klassifikasjoner/632)
+ [Korrespondansetabell mellom `KLASS 629` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/629/korrespondanser/1026)
+ [Korrespondansetabell mellom `KLASS 630` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/630/korrespondanser/1046)
+ [Korrespondansetabell mellom `KLASS 631` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/631/korrespondanser/1048)
+ [Korrespondansetabell mellom `KLASS 631` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/632/korrespondanser/1098)


## Opptaksområder

[Opptaksområder](https://statisticsnorway.github.io/speshelse/)
