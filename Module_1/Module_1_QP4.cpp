// Pin definitions 
const int motionSensor  = 2;   // PIR 1 
const int tiltSensor    = 3;   // Tilt switch 
const int motionSensor2 = 4;   // PIR 2 
const int buzzer        = 7;   // Voltage buzzer not monitored by PCI
const int led           = 13;  // Red LED
const int ledBlue       = 11;  // Blue LED
const uint8_t ledGreen  = PB4; // Green LED - direct port manipulation (Arduino D12)

//Sensor state 
volatile uint8_t motionState  = 0;
volatile uint8_t tiltState    = 0;
volatile uint8_t motionState2 = 0;

// Previous states (main loop only, for edge detection)
uint8_t prevMotion  = 0;
uint8_t prevTilt    = 0;
uint8_t prevMotion2 = 0;

// Timer flag 
volatile bool timerFlag = false;

// Setup functions
void setupPins() {
  pinMode(buzzer, OUTPUT);
  pinMode(led, OUTPUT);
  pinMode(ledBlue, OUTPUT);
  pinMode(tiltSensor, INPUT_PULLUP);
  pinMode(motionSensor, INPUT);
  pinMode(motionSensor2, INPUT);
  DDRB |= (1 << ledGreen);   // green LED as output via direct port register
}

void setupPCINT() {
  // Enable PCINT for PORTD (D0-D7)
  PCICR |= (1 << PCIE2);

  // Watch D2, D3, D4
  PCMSK2 |= (1 << PCINT18) | (1 << PCINT19) | (1 << PCINT20);
}

void setupTimer1() {
  noInterrupts();
  TCCR1A = 0;
  TCCR1B = 0;
  TCNT1  = 0;

  OCR1A = 31250;                          // 2s at 16MHz with prescaler 1024
  TCCR1B |= (1 << WGM12);                 // CTC mode
  TCCR1B |= (1 << CS12) | (1 << CS10);    

  TIMSK1 |= (1 << OCIE1A);                // Enable Timer1 compare interrupt
  interrupts();
}

// Motion sensor1
void handleMotionSensor1() {
  if (prevMotion != motionState) {
    digitalWrite(led, motionState ? HIGH : LOW);
    Serial.println(motionState ? "Motion Sensor 1: motion detected - Red LED ON"
                                : "Motion Sensor 1: clear - Red LED OFF");
    prevMotion = motionState;
  }
}

// Motion sensor2
void handleMotionSensor2() {
  if (prevMotion2 != motionState2) {
    digitalWrite(ledBlue, motionState2 ? HIGH : LOW);
    Serial.println(motionState2 ? "Motion Sensor 2: motion detected - Blue LED ON"
                                 : "Motion Sensor 2: clear - Blue LED OFF");
    prevMotion2 = motionState2;
  }
}

// Tilt sensor
void handleTiltSensor() {
  if (prevTilt != tiltState) {
    digitalWrite(buzzer, tiltState ? HIGH : LOW);
    Serial.println(tiltState ? "Tilt Sensor: orientation changed - Buzzer ON"
                              : "Tilt Sensor: level - Buzzer OFF");
    prevTilt = tiltState;
  }
}

void handleHeartbeat() {
  if (timerFlag) {
    timerFlag = false;
    PORTB ^= (1 << ledGreen);   // toggle green LED
    Serial.println("Timer1: heartbeat tick - Green LED toggled");
  }
}

// Arduino setup
void setup() {
  Serial.begin(9600);
  setupPins();
  setupPCINT();
  setupTimer1();
}

void loop() {
  handleMotionSensor1();
  handleMotionSensor2();
  handleTiltSensor();
  handleHeartbeat();
}

// Interrupt Service 
// Single PCINT2 vector handles all 3 sensors
ISR(PCINT2_vect) {
  motionState  = PIND & (1 << PD2);  // D2
  tiltState    = PIND & (1 << PD3);  // D3
  motionState2 = PIND & (1 << PD4);  // D4
}

// Timer1
ISR(TIMER1_COMPA_vect) {
  timerFlag = true;
}