use rppal::gpio::{Gpio, Trigger};
use rumqttc::{MqttOptions, Client, Packet, Event, QoS};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use log::{info, debug, error};
use env_logger::Env;

// --- Konfiguration ---
// Passen Sie diese Werte an Ihre Umgebung an
const GPIO_PIN: u8 = 22;
const MQTT_BROKER_ADRESSE: &str = "192.168.2.3"; // z.B. "localhost" oder "192.168.1.10"
const MQTT_PORT: u16 = 1893;
const MQTT_CLIENT_ID: &str = "rpi-gaszaehler";

// MQTT-Topics
const WERT_TOPIC: &str = "haus/gaszaehler/stand";
const SETZE_TOPIC: &str = "haus/gaszaehler/set_stand";

const DIFFERENZ_TOPIC: &str = "haus/gaszaehler/verbrauch";
const DIFFERENZ_TOPIC_KWH: &str = "haus/gaszaehler/verbrauch-kwh";

// Entprell-Zeit in Millisekunden (verhindert doppeltes Zählen bei Tastendruck)
const DEBOUNCE_MS: u64 = 500;
// --- Ende Konfiguration ---
const BRENNWERT: f64 = 11.50;
const ZUSTANDSWERT: f64 = 0.9643;
// Hilfsfunktion (unverändert)
fn berechne_zeit_bis_naechste_stunde() -> (u64, String) {
    let jetzt = SystemTime::now();
    let dauer_seit_epoche = jetzt
        .duration_since(UNIX_EPOCH)
        .expect("Die Zeit liegt vor der UNIX-Epoche.");
    let sekunden_seit_epoche = dauer_seit_epoche.as_secs();
    const SEKUNDEN_PRO_STUNDE: u64 = 3600;
    let sekunden_in_dieser_stunde = sekunden_seit_epoche % SEKUNDEN_PRO_STUNDE;
    let sekunden_bis_zur_stunde = SEKUNDEN_PRO_STUNDE - sekunden_in_dieser_stunde + 1;
    let naechste_stunde_utc = (sekunden_seit_epoche / SEKUNDEN_PRO_STUNDE + 1) % 24;
    (sekunden_bis_zur_stunde, format!("{:02}:00 UTC", naechste_stunde_utc))
}

fn main() {
    let env = Env::default()
        .filter_or("GASZAEHLER_LOG_LEVEL", "trace")
        .write_style_or("GASZAEHLER_LOG_STYLE", "always");

    env_logger::init_from_env(env);

    // Shared State (Atomarer Referenzzähler + Mutex) für den Zählerwert.
    // Arc: Ermöglicht es, den Zähler sicher zwischen Threads zu teilen.
    // Mutex: Stellt sicher, dass immer nur ein Thread gleichzeitig den Wert ändert.
    let zaehler_shared = Arc::new(Mutex::new(0.0f64));
    // Shared State für den Zählerwert der letzten vollen Stunde.
    let letzter_stunden_wert_shared = Arc::new(Mutex::new(0.0f64));

    // NEU: Atomic-Flag, um zu prüfen, ob der Startwert vom Broker geladen wurde.
    // Dies verhindert, dass wir unseren eigenen (neu gesendeten) Wert als "Startwert" lesen.
    let is_initialized = Arc::new(AtomicBool::new(false));

    // --- MQTT-Setup ---
    let mut mqtt_options = MqttOptions::new(MQTT_CLIENT_ID, MQTT_BROKER_ADRESSE, MQTT_PORT);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_credentials("fhem","Sonne2020");

    let (client, mut connection) = Client::new(mqtt_options, 10);
    // Abonniere das "setze"-Topic
    client.subscribe(SETZE_TOPIC, QoS::AtLeastOnce).expect("MQTT subscribe failed");
    client.subscribe(WERT_TOPIC, QoS::AtLeastOnce).expect("MQTT subscribe failed (wert)");

    info!("Verbunden mit MQTT-Broker auf {} und abonniert auf '{}' und '{}'", MQTT_BROKER_ADRESSE, SETZE_TOPIC, WERT_TOPIC);
    // Klone für den GPIO-Thread
    let client_fuer_gpio = client.clone();
    let zaehler_fuer_gpio = zaehler_shared.clone();
    let wert_topic_str = WERT_TOPIC.to_string(); // Topic-String für den Thread klonen

    let client_fuer_stunde = client.clone();
    let zaehler_fuer_stunde = zaehler_shared.clone();
    let letzter_wert_fuer_stunde = letzter_stunden_wert_shared.clone();
    let differenz_topic_str = DIFFERENZ_TOPIC.to_string();
    let differenz_topic_kwh_str = DIFFERENZ_TOPIC_KWH.to_string();

    // Klone für die MQTT-Schleife
    let zaehler_fuer_mqtt = zaehler_shared.clone();
    let letzter_wert_fuer_mqtt = letzter_stunden_wert_shared.clone();
    let is_initialized_fuer_mqtt = is_initialized.clone();

    // --- GPIO-Thread ---
    // Dieser Thread kümmert sich ausschließlich um das Abhören des GPIO-Pins.
    thread::spawn(move || {
        // Initialisiere GPIO
        let gpio = Gpio::new().expect("GPIO-Initialisierung fehlgeschlagen");
        let mut pin = gpio.get(GPIO_PIN)
            .expect(&format!("Pin {} konnte nicht abgerufen werden", GPIO_PIN))
            .into_input_pullup(); // Pin als Input mit Pull-Up konfigurieren

        // Interrupt für fallende Flanke (High → Low)
        // Da Pull-Up, ist der Pin normal auf HIGH. Ein Signal (z.B. Taster) zieht ihn auf LOW.
        pin.set_interrupt(Trigger::FallingEdge,Some(Duration::from_millis(50))).expect("Interrupt-Setup fehlgeschlagen");

        let mut letzte_ausloesung = Instant::now();
        let debounce_dauer = Duration::from_millis(DEBOUNCE_MS);

        debug!("[GPIO] Warte auf Signale auf Pin {}...", GPIO_PIN);

        loop {
            // Blockiere, bis ein Interrupt (fallende Flanke) auftritt
            match pin.poll_interrupt(true, None) { // `true` = clear interrupt, `None` = blockiere unendlich
                Ok(Some(_level)) => { // Interrupt ausgelöst
                    let jetzt = Instant::now();
                    // Entprell-Logik: Nur fortfahren, wenn genug Zeit seit der letzten Auslösung vergangen ist
                    if jetzt.duration_since(letzte_ausloesung) > debounce_dauer {
                        letzte_ausloesung = jetzt; // Zeitstempel aktualisieren

                        let aktueller_wert: f64;
                        { // --- Mutex Lock Scope ---
                            // Sperre den Zähler, um ihn sicher zu erhöhen
                            let mut zaehler_lock = zaehler_fuer_gpio.lock().unwrap();
                            *zaehler_lock += 0.01;
                            aktueller_wert = *zaehler_lock;
                        } // --- Mutex wird hier automatisch freigegeben ---

                        debug!("[GPIO] Zähler erhöht auf: {:.2}", aktueller_wert);

                        // Sende den neuen Wert per MQTT
                        let payload = format!("{:.2}", aktueller_wert);
                        if let Err(e) = client_fuer_gpio.publish(
                            &wert_topic_str, // Benutze den geklonten String
                            QoS::AtLeastOnce, // Sende mindestens einmal (zuverlässig)
                            true,            // false = keine "retained" message
                            payload
                        ) {
                            error!("[GPIO] MQTT Sende-Fehler: {}", e);
                        }
                    }
                }
                Ok(None) => { /* Timeout, sollte hier nicht passieren */ }
                Err(e) => error!("[GPIO] Interrupt Poll-Fehler: {}", e),
            }
        }
    });
    // --- Stunden-Thread ---
    thread::spawn(move || {
        loop {
            let (sekunden_bis_zur_stunde, stunde_str) = berechne_zeit_bis_naechste_stunde();
            debug!("[Stunde] Schlafe für {} Sekunden (bis ca. {}).", sekunden_bis_zur_stunde, stunde_str);
            thread::sleep(Duration::from_secs(sekunden_bis_zur_stunde));
            debug!("[Stunde] Aufgewacht für die stündliche Meldung.");

            let aktueller_wert: f64;
            let letzter_wert: f64;
            let differenz: f64;
            let differenz_kwh: f64;

            {
                let zaehler_lock = zaehler_fuer_stunde.lock().unwrap();
                aktueller_wert = *zaehler_lock;
            }

            {
                let mut letzter_wert_lock = letzter_wert_fuer_stunde.lock().unwrap();
                letzter_wert = *letzter_wert_lock;
                differenz = aktueller_wert - letzter_wert;
                *letzter_wert_lock = aktueller_wert;
            }

            debug!("[Stunde] Sende Differenz: {:.2} (Aktuell: {:.2}, Letzte Stunde: {:.2})", differenz, aktueller_wert, letzter_wert);

            let payload = format!("{:.2}", differenz);
            // Senden der Differenz (nicht retained)
            if let Err(e) = client_fuer_stunde.publish(
                &differenz_topic_str,
                QoS::AtLeastOnce,
                false, // Differenz soll nicht retained sein
                payload
            ) {
                error!("[Stunde] MQTT Sende-Fehler: {}", e);
            }
            differenz_kwh = differenz * BRENNWERT * ZUSTANDSWERT;
            let payload_kwh = format!("{:.2}", differenz_kwh);
            // Senden der verbrauch in Kwh (nicht retained)
            if let Err(e) = client_fuer_stunde.publish(
                &differenz_topic_kwh_str,
                QoS::AtLeastOnce,
                false, // Differenz soll nicht retained sein
                payload_kwh
            ) {
                error!("[Stunde] MQTT Sende-Fehler: {}", e);
            }
        }
    });
    // --- MQTT-Event-Loop (im Haupt-Thread) ---
    // Diese Schleife kümmert sich um eingehende MQTT-Nachrichten (z.B. "setze")
    // und hält die Verbindung aufrecht.
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Packet::Publish(publish))) => {

                // --- NEU: Logik zum Laden des Startwerts ---
                // Prüft, ob es eine Nachricht auf WERT_TOPIC ist UND ob wir noch nicht initialisiert sind.
                if publish.topic == WERT_TOPIC && !is_initialized_fuer_mqtt.load(Ordering::SeqCst) {
                    if let Ok(payload_str) = std::str::from_utf8(&publish.payload) {
                        if let Ok(retained_wert) = payload_str.parse::<f64>() {

                            { // --- Mutex Lock Scope ---
                                let mut zaehler_lock = zaehler_fuer_mqtt.lock().unwrap();
                                *zaehler_lock = retained_wert;

                                let mut letzter_wert_lock = letzter_wert_fuer_mqtt.lock().unwrap();
                                *letzter_wert_lock = retained_wert;
                            } // --- Mutex wird hier freigegeben ---

                            // Setze Flag, damit dies nur einmal passiert
                            is_initialized_fuer_mqtt.store(true, Ordering::SeqCst);
                            info!("[MQTT] Startwert von Broker wiederhergestellt: {:.2}", retained_wert);
                        }
                    }
                }
                // --- Logik zum Setzen des Werts (wie zuvor) ---
                else if publish.topic == SETZE_TOPIC {
                    if let Ok(payload_str) = std::str::from_utf8(&publish.payload) {
                        if let Ok(neuer_wert) = payload_str.parse::<f64>() {

                            { // --- Mutex Lock Scope ---
                                let mut zaehler_lock = zaehler_fuer_mqtt.lock().unwrap();
                                *zaehler_lock = neuer_wert;

                                let mut letzter_wert_lock = letzter_wert_fuer_mqtt.lock().unwrap();
                                *letzter_wert_lock = neuer_wert;
                            }

                            // NEU: Wenn jemand den Wert setzt, gilt das auch als "initialisiert".
                            is_initialized_fuer_mqtt.store(true, Ordering::SeqCst);
                            info!("[MQTT] Zähler gesetzt auf: {:.2} (Stündliche Differenz zurückgesetzt)", neuer_wert);

                            // Sende den neu gesetzten *absoluten* Wert (als retained)
                            let payload = format!("{:.2}", neuer_wert);
                            client.publish(
                                WERT_TOPIC,
                                QoS::AtLeastOnce,
                                true, // MODIFIZIERT: Retained Flag auf true setzen
                                payload
                            ).unwrap_or_else(|e| error!("[MQTT] Sende-Fehler nach Setzen: {}", e));
                        } else {
                            error!("[MQTT] Ungültiger Payload auf '{}': {}", SETZE_TOPIC, payload_str);
                        }
                    }
                }
            }
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("[MQTT] Wieder verbunden.");
                // Falls die Verbindung abbricht, müssen wir sicherstellen, dass das 'is_initialized' Flag
                // zurückgesetzt wird, falls der Broker in der Zwischenzeit neu gestartet wurde und den Wert verloren hat.
                // Für Einfachheit lassen wir es aber erstmal gesetzt.
                // Eine robustere Lösung würde 'is_initialized' nur setzen, wenn die ConnAck eine 'clean session=false' bestätigt.
            }
            Ok(Event::Incoming(Packet::SubAck(_))) => {
                info!("[MQTT] Abonnements bestätigt.");
                // Wenn wir Abos bestätigt bekommen und *noch nicht* initialisiert sind,
                // könnte der Broker keine retained Message haben. Wir setzen hier einen Timeout
                // von z.B. 2 Sekunden. Wenn bis dahin keine Message kam, setzen wir 'is_initialized = true'.

                let is_init_clone = is_initialized.clone();
                let zaehler_clone = zaehler_shared.clone();
                let letzter_wert_clone = letzter_stunden_wert_shared.clone();

                thread::spawn(move || {
                    thread::sleep(Duration::from_secs(2)); // Warte 2 Sekunden auf die retained Message

                    // Vergleiche und setze (compare-and-swap)
                    // Setze 'true' nur, wenn es vorher 'false' war.
                    if is_init_clone.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                        // Dieser Block wird ausgeführt, WENN 'is_initialized' false WAR und jetzt true ist
                        // (d.h. wir haben 2s gewartet und keine retained msg kam)
                        let val;
                        {
                            val = *zaehler_clone.lock().unwrap();
                        }
                        info!("[MQTT] Kein Startwert vom Broker empfangen. Starte mit dem initialen Wert: {:.2}", val);
                        // Der Wert ist bereits 0.0 (oder was auch immer initial gesetzt wurde),
                        // aber wir müssen den 'letzter_stunden_wert' synchronisieren.
                        {
                            *letzter_wert_clone.lock().unwrap() = val;
                        }
                    }
                });
            }
            Err(e) => {
                error!("[MQTT] Verbindungsfehler: {}. Versuche erneut...", e);
                thread::sleep(Duration::from_secs(1));
            }
            _ => { /* Andere Events ignorieren */ }
        }
    }
}
