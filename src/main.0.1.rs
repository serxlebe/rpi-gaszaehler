use rppal::gpio::{Gpio, Trigger};
use rumqttc::{MqttOptions, Client, Packet, Event, QoS};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// --- Konfiguration ---
// Passen Sie diese Werte an Ihre Umgebung an
const GPIO_PIN: u8 = 22;
const MQTT_BROKER_ADRESSE: &str = "192.168.2.3"; // z.B. "localhost" oder "192.168.1.10"
const MQTT_PORT: u16 = 1893;
const MQTT_CLIENT_ID: &str = "rpi-gaszaehler";

// MQTT-Topics
const WERT_TOPIC: &str = "haus/gaszaehler/stand";
const SETZE_TOPIC: &str = "haus/gaszaehler/set_stand";

// Entprell-Zeit in Millisekunden (verhindert doppeltes Zählen bei Tastendruck)
const DEBOUNCE_MS: u64 = 500;
// --- Ende Konfiguration ---

fn main() {
    // Shared State (Atomarer Referenzzähler + Mutex) für den Zählerwert.
    // Arc: Ermöglicht es, den Zähler sicher zwischen Threads zu teilen.
    // Mutex: Stellt sicher, dass immer nur ein Thread gleichzeitig den Wert ändert.
    let zaehler_shared = Arc::new(Mutex::new(0.0f64));

    // --- MQTT-Setup ---
    let mut mqtt_options = MqttOptions::new(MQTT_CLIENT_ID, MQTT_BROKER_ADRESSE, MQTT_PORT);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_credentials("fhem","Sonne2020");

    let (client, mut connection) = Client::new(mqtt_options, 10);
    // Abonniere das "setze"-Topic
    client.subscribe(SETZE_TOPIC, QoS::AtLeastOnce).expect("MQTT subscribe failed");
    println!("Verbunden mit MQTT-Broker auf {} und abonniert auf '{}'", MQTT_BROKER_ADRESSE, SETZE_TOPIC);

    // Klone für den GPIO-Thread
    let client_fuer_gpio = client.clone();
    let zaehler_fuer_gpio = zaehler_shared.clone();
    let wert_topic_str = WERT_TOPIC.to_string(); // Topic-String für den Thread klonen

    // --- GPIO-Thread ---
    // Dieser Thread kümmert sich ausschließlich um das Abhören des GPIO-Pins.
    thread::spawn(move || {
        // Initialisiere GPIO
        let gpio = Gpio::new().expect("GPIO-Initialisierung fehlgeschlagen");
        let mut pin = gpio.get(GPIO_PIN)
            .expect(&format!("Pin {} konnte nicht abgerufen werden", GPIO_PIN))
            .into_input_pullup(); // Pin als Input mit Pull-Up konfigurieren

        // Interrupt für fallende Flanke (High -> Low)
        // Da Pull-Up, ist der Pin normal auf HIGH. Ein Signal (z.B. Taster) zieht ihn auf LOW.
        pin.set_interrupt(Trigger::FallingEdge,Some(Duration::from_millis(50))).expect("Interrupt-Setup fehlgeschlagen");

        let mut letzte_ausloesung = Instant::now();
        let debounce_dauer = Duration::from_millis(DEBOUNCE_MS);

        println!("[GPIO] Warte auf Signale auf Pin {}...", GPIO_PIN);

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

                        println!("[GPIO] Zähler erhöht auf: {:.2}", aktueller_wert);

                        // Sende den neuen Wert per MQTT
                        let payload = format!("{:.2}", aktueller_wert);
                        if let Err(e) = client_fuer_gpio.publish(
                            &wert_topic_str, // Benutze den geklonten String
                            QoS::AtLeastOnce, // Sende mindestens einmal (zuverlässig)
                            false,            // false = keine "retained" message
                            payload
                        ) {
                            println!("[GPIO] MQTT Sende-Fehler: {}", e);
                        }
                    }
                }
                Ok(None) => { /* Timeout, sollte hier nicht passieren */ }
                Err(e) => println!("[GPIO] Interrupt Poll-Fehler: {}", e),
            }
        }
    });

    // --- MQTT-Event-Loop (im Haupt-Thread) ---
    // Diese Schleife kümmert sich um eingehende MQTT-Nachrichten (z.B. "setze")
    // und hält die Verbindung aufrecht.
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                // Prüfe, ob die Nachricht auf unserem "setze"-Topic kam
                if publish.topic == SETZE_TOPIC {
                    // Konvertiere Payload (Byte-Array) in einen String
                    if let Ok(payload_str) = std::str::from_utf8(&publish.payload) {
                        // Versuche, den String in eine f64 (Zahl) zu parsen
                        if let Ok(neuer_wert) = payload_str.parse::<f64>() {
                            { // --- Mutex Lock Scope ---
                                let mut zaehler_lock = zaehler_shared.lock().unwrap();
                                *zaehler_lock = neuer_wert;
                            } // --- Mutex wird hier freigegeben ---

                            println!("[MQTT] Zähler gesetzt auf: {:.2}", neuer_wert);

                            // Sende den neu gesetzten Wert ebenfalls, um die Änderung zu bestätigen
                            // (erfüllt "nur bei Änderung senden")
                            let payload = format!("{:.2}", neuer_wert);
                            client.publish(WERT_TOPIC, QoS::AtLeastOnce, false, payload)
                                .unwrap_or_else(|e| println!("[MQTT] Sende-Fehler nach Setzen: {}", e));
                        } else {
                            println!("[MQTT] Ungültiger Payload auf '{}': {}", SETZE_TOPIC, payload_str);
                        }
                    }
                }
            }
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                println!("[MQTT] Wieder verbunden.");
            }
            Err(e) => {
                println!("[MQTT] Verbindungsfehler: {}. Versuche erneut...", e);
                thread::sleep(Duration::from_secs(1)); // Kurz warten vor dem nächsten Verbindungsversuch
            }
            _ => { /* Andere Events (wie Ping/Pong) ignorieren */ }
        }
    }
}
