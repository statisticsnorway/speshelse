# Config (Norsk)

Alle produksjonsløp trenger ett sted å lagre konfigurasjoenr/instillinger. 
Denne mappen Skal inneholde alle konfigurasjon filer som er nødvendig for drift av produksjon og analyse.

## .toml Filer
For å standardisere hvilken filstruktur som benyttes, 
Anbefaler vi så langt det lar seg gjøre og benytte språkagnostiske instillingsfiler.
Dette er slik at uavhengig av hvilket språk som er brukt kan de benytte seg av disse instillingen. 
Vi anbefaler bruken av `.toml` filer. 
Disse har en enkel oppbygnign, og er fulstendig språk agnostiske. 

## feature_gates.toml
Denne filen legger til rette for å kunne styre om produksjonen kjøres i ett utviklings miljø,
eller aktiv produksjon.

hovedflagget skal stå på prod, når koden kjøres i produkjon, 
og dev nar du jobber på testing og utvikling. 
Dette gjøres for å ikke påvirke produksjons data mens du utvikler.

Det er imidlertid slik at selv om du er i Produksjon, 
så kan det være at du ønsker og holde deler av produksjonen ikke aktiv "dev" modus. 
Ved å gi de individuelle prossessene egene feature gates, 
kan de integreres i produksjons koden, men beholdes som inaktiv intill tidspunktet egner seg.


## filstier.toml
DAPLA er under utvikling. Dette betyr at ikke alle funksjonaliteter er på plass. 
en av de tingene som enda er litt kronglete er identifisering av aktive filstier.
I tillegg vil Produksjon ofte trekke på de samme filsteiene, i forskellige steder av produksjon.
For å ungå den manuelle prosessen med å oppdatere hvert sted der filstien er i bruk,
anbefaler vi å benytte en konfigurasjons fil som older kontroll på de fysiske filstiene, 
og tildeler dem en `"Symlink"` representasjon i koden som tolkes dynamisk i kjørings tidspunktet.

Repoet inneholder en fil som er starten for dette.

En kommentar rundt å gjøre dette mere robust, 
er at dersom i filstier så er symlinken knyttet til det mest statiske delen av filstien.
Kombiner filstier sammen med feature_gate instillingen for å bytte mellom utviklings kjøringen, 
og langtids lagrede kjøringer