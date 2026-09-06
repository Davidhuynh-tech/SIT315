# TaskM1QP — Interrupt-Based Sense-Think-Act System (QP4 / Distinction)

## Summary
An Arduino Uno system demonstrating a full interrupt-driven Sense-Think-Act architecture, combining Pin Change Interrupts (PCINT) across three digital sensors with an independent Timer1-based periodic task.

**Sensors:**
- PIR Motion Sensor 1 → D2
- Tilt Switch → D3
- PIR Motion Sensor 2 → D4

**Actuators:**
- Red LED → D13 (Motion Sensor 1 indicator)
- Blue LED → D11 (Motion Sensor 2 indicator)
- Buzzer → D7 (Tilt Switch indicator)
- Green LED → D12 (Timer1 heartbeat, toggles every 2 seconds independent of sensors)

## Interrupt Design
- All three sensors sit on PORTD (D2–D4), so a single `PCINT2_vect` ISR services all of them. The ISR reads `PIND` once and masks each sensor's bit individually to determine which pin changed.
- Timer1 runs in CTC mode (prescaler 1024) and fires `TIMER1_COMPA_vect` every 2 seconds, completely independent of sensor activity — proving the system handles concurrent event-based and time-based interrupts without blocking.
- No `delay()` or blocking calls appear in either ISR; both only set flags/state, with all actual logic and actuation handled in `loop()`.

## Files
- `TaskM1.cpp` — full Arduino source code
- `circuit-diagram/` — TinkerCad schematic and breadboard view
- `reflection-report.md` — architecture, interrupt configuration, and issues/resolutions (300–500 words)

## How to Run
1. Open the project in TinkerCad Circuits (or wire it physically per the circuit diagram).
2. Paste `TaskM1.cpp` into the code editor (Code → Text).
3. Start the simulation and open the Serial Monitor (9600 baud).
4. Trigger PIR1, PIR2, and the tilt switch individually and in combination to observe independent sensor responses.
5. Observe the green LED (D12) toggling every 2 seconds throughout, regardless of sensor state — this confirms the Timer1 interrupt runs concurrently with the PCINT-driven sensor logic.

## Test Conditions Demonstrated
1. PIR1 triggered alone
2. PIR1 and PIR2 triggered together
3. All three sensors triggered simultaneously while Timer1 continues running

## Wiring Reference
| Component        | Arduino Pin |
|-------------------|-------------|
| PIR 1 (OUT)       | D2          |
| Tilt Switch       | D3          |
| PIR 2 (OUT)       | D4          |
| Buzzer            | D7          |
| Blue LED          | D11         |
| Green LED         | D12         |
| Red LED           | D13         |
