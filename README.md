
# Spesialisthelsetjenesten

Her finnes programmer som brukes i produksjonssløp for statistikkene som inngår i [Spesialisthelsetjenesten](https://www.ssb.no/helse/helsetjenester/statistikk/spesialisthelsetjenesten). 

Spesialisthelsetjensten er inndelt i følgende statistikkområder:
+ [Spesialisthelsetjenesten - Regnskap](https://github.com/statisticsnorway/stat-speshelse-regnskap)
+ [Spesialisthelsetjenesten - Personell](https://github.com/statisticsnorway/stat-speshelse-personell)
+ [Spesialisthelsetjenesten - Aktivitet og tjenester](https://github.com/statisticsnorway/stat-speshelse-aktivitet)

I tillegg ligger programmer relatert til geografiske analyser (GIS) her:
+ [Spesialisthelsetjenesten - Geografiske analyser (GIS)](https://github.com/statisticsnorway/stat-speshelse-gis)

## Årshjul

I dette repoet ligger hovedsakelig programmer som er felles for alle produksjonsløpene i statistikkområdene. Faste oppgaver knyttet til statistikk
for spesialisthelsetjenester er følgende:

### Januar
+ **Uke 2:** Informasjonsbrev til rapportører
+ Oppdatere samarbeidsavtale med Samdata, og sende signert versjon til HOD.\
  *Samarbeidsavtalen er lagret her: Statistisk sentralbyrå\S330 - Dokumenter\SPESHELSE\9. Støtte og infrastruktur\9.0 Administrasjon*
+ Kalle inn til årets arbeidsgruppemøter
### Februar
+ Innrapporteringsfrist **20. februar** for døgnplass-skjemaer, ambulanse, samhandlingsleger og avtalespesialister.
+ Sende påminnelse til rapportører én uke før frist.
+ Samarbeidsmøte om døgnplass-data med Helsedirektoratet.
+ Sende purring på skjema med frist 20.februar, cirka én uke etter fristen. [Purreprogrammer](https://github.com/statisticsnorway/speshelse/tree/master/experimental)
### Mars
+ **Uke 10:** Første filoverføring av døgnplass-data til Helsedirektoratet. [Program for å lage filer](Samdata)
+ **Uke 11:** Sende andre purring på skjemaer som hadde frist 20.februar.
+ **Uke 12:** Sende påminnelse om regnskaprapportering
+ **Uke 12:** [Melde publisering av spesialisthelsetjeneste-statistikken i uke 25](https://ssbno.sharepoint.com/sites/Kommunikasjonogpublisering/SitePages/Statistikksiden-(ssb.no).aspx)
+ Publisere foreløpige tall i [personell-tabellene](https://github.com/statisticsnorway/stat-speshelse-personell.git).
+ **Uke 13:** Planleggingsmøte om regnskapsdata med Helsedirektoratet.
### April
+ Frist for regnskapsrapportering:\
  **1.april:** HF og private med oppdragsdokument\
  **15.april:** RHF
+ **Uke 15:** Første [filoverføring](Samdata) av regnskapsdata til Helsedirektoratet.
+ **Uke 16:** Andre filoverføring av døgnplassdata til Helsedirektoratet.
+  Planlegge arbeidsgruppemøte i mai/juni. Formøte med HOD én måned før møtet.\
 *Relevante dokumenter finnes her: Statistisk sentralbyrå\S330 - Dokumenter\SPESHELSE\9. Støtte og infrastruktur\9.3 Møter*
### Mai
+ Arbeidsgruppemøte i mai/juni. Sakspapirer og agenda sendes ut senest to uker før møtet.
+ **Uke 19:** Samarbeidsmøte om døgnplass-data med Helsedirektoratet.
+ **Uke 19 og 21:** Samarbeidsmøte om regnskapsdata med Helsedirektoratet.
+ Møter med RHF-ene om elimineringer i regnskapene.
+ **Uke 22:** Overføre endelige døgnplass-filer til Helsedirektoratet.
### Juni
+ Konsolideringsmøte om regnskapsdata – sammenligne tall med Helsedirektoratet
+ **Uke 24:** Overføre endelige regnskapsfiler til Helsedirektoratet.
+ **Uke 25:** Publisere statistikk for spesialisthelsetjenester.
+ Lage release på Github av koden som ble brukt til publiseringen.
+ Låse delregisteret som ble brukt i publiseringen.
+ Sende ut referat fra arbeidsgruppemøtet senest to uker etter møtet.

### August
+ [Etablere delregister](https://statistics-norway.atlassian.net/wiki/spaces/REIS/pages/3919413938/Opprett+nytt+delregister+i+VoF)
+ [Sende foreløpig populasjon til Helsedirektoratet](https://github.com/statisticsnorway/speshelse/tree/master/Institusjonslister)
+ Planlegge arbeidsgruppemøte i september. Formøte med HOD én måned før møtet.\
 *Relevante dokumenter finnes her: Statistisk sentralbyrå\S330 - Dokumenter\SPESHELSE\9. Støtte og infrastruktur\9.3 Møter*
### September
+ Arbeidsgruppemøte. Sakspapirer og agenda sendes ut senest to uker før møtet.
### Oktober
+ Sende ut referat fra arbeidsgruppemøtet innen to uker etter møtet.
+ Oppdatere delregisteret med private institusjon som har avtale med RHF\
  Informasjon om avtalene ligger på RHF-enes nettsider.\
  [Program som gir oversikt som kan være nyttig når vi oppdaterer delregisteret](https://github.com/statisticsnorway/speshelse/blob/master/experimental/populasjon_sammenlikne_ny_og_gammel.py)
+ [Tilrettelegge for ny rapportering i Hubbus](https://statistics-norway.atlassian.net/wiki/spaces/KOSTRA/pages/4148133900/Tilrettelegge+for+ny+rapportering+i+Hubbus)
+ [Lage prefill og brukerlister](Droplister)
+ Evalueringsmøte med Helsedirektoratet
  
### November
+ Vi tester skjemaene som er i [innrapporteringsportalen](https://www.ssb.no/innrapportering/helseforetak). Testperioden er de to første ukene i november.\
  [Veiledning til testingen finnes her](https://statistics-norway.atlassian.net/wiki/spaces/KOSTRA/pages/5189042443/Testperiode+2026)
+ Lage [produksjonspassord](https://github.com/statisticsnorway/speshelse/blob/master/Droplister/Brukerlister.py) til rapporteringsperioden til skjemaene som rapporteres i innrapporteringsportalen.
  
### Desember
+ Oppdatere delregisterene med passord til Altinn-rapporteringen: (26779, 26512, 26492, 26778, 26816)
+ Oppdatere [innrapporteringssiden](https://www.ssb.no/innrapportering/helseforetak). Det gjøres i [Enonic](https://i.ssb.no/xp/admin/tool)
+ Ferdigstille brev til rapportørene
+ Ferdigstille spesifikasjonsskjemaene for HF og RHF, og få lagt dem inn i Altinn.
+ Oppdatere databehandleravtale med HOD\
  *Databehandleravtalene er lagret her: S330 - Dokumenter\SPESHELSE\9. Støtte og infrastruktur\9.1 Juridisk overbygning*

## KLASS

Før produksjonsløpene kan kjøres må alle kodelistene som omfatter spesialisthelsetjenesten være oppdatert for statistikkåret. 

### Felles
+ `KLASS 603`: [Standard for offentlige helseforetak](https://www.ssb.no/klass/klassifikasjoner/603)
+ `KLASS 604`: [Standard for private helseinstitusjoner med oppdrags- og bestillerdokument](https://www.ssb.no/klass/klassifikasjoner/604)
+ `KLASS 605`: [Kodeliste for regionale og felleseide støtteforetak i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/605)
+ `KLASS 610`: [Kodeliste for tjenesteområder i spesialisthelsetjenesten](https://www.ssb.no/klass/klassifikasjoner/610)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 6`](https://www.ssb.no/klass/klassifikasjoner/610/korrespondanser/898)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 603`](https://www.ssb.no/klass/klassifikasjoner/603/korrespondanser/1320)
+ [Korrespondansetabell mellom `KLASS 610` og `KLASS 604`](https://www.ssb.no/klass/klassifikasjoner/604/versjon/3312/korrespondanser/2797)

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

#### Korrespondanse til kommuneinndeling
+ [Korrespondansetabell mellom `KLASS 603` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/603/korrespondanser/2577)
+ [Korrespondansetabell mellom `KLASS 629` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/629/korrespondanser/1026)
+ [Korrespondansetabell mellom `KLASS 630` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/630/korrespondanser/1046)
+ [Korrespondansetabell mellom `KLASS 631` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/631/korrespondanser/1048)
+ [Korrespondansetabell mellom `KLASS 632` og `KLASS 131`](https://www.ssb.no/klass/klassifikasjoner/632/korrespondanser/1098)

#### Korrespondanse til bydelsinndeling
+ [Korrespondansetabell mellom `KLASS 629` og `KLASS 103`](https://www.ssb.no/klass/klassifikasjoner/629/korrespondanser/2519)
+ [Korrespondansetabell mellom `KLASS 630` og `KLASS 103`](https://www.ssb.no/klass/klassifikasjoner/630/korrespondanser/2520)
+ [Korrespondansetabell mellom `KLASS 631` og `KLASS 103`](https://www.ssb.no/klass/klassifikasjoner/631/korrespondanser/2521)
+ [Korrespondansetabell mellom `KLASS 632` og `KLASS 103`](https://www.ssb.no/klass/klassifikasjoner/632/korrespondanser/2522)

#### Korrespondanse til postnummerområder 
+ [Korrespondansetabell mellom `KLASS 632` og `KLASS 616`](https://www.ssb.no/klass/klassifikasjoner/616/korrespondanser/2524)


## Kart over opptaksområder

[Opptaksområder](https://statisticsnorway.github.io/speshelse/)

