//! What a Dimplex heater of the Remo app expects over Bluetooth LE, as found on an EVO2BTA. Pure,
//! so that it is tested on every platform.

/// The mode byte of a heater that heats to the setpoint until its power is cut.
const MODE_MANUAL: u8 = 2;

/// The length of the mode frame.
const FRAME_LEN: usize = 6;

/// The passkey a heater pairs with, derived from the last three bytes of its `address` as the
/// Remo app does: each hex digit counts as a decimal one, `A` to `F` as 0 to 5, each is replaced
/// by 9 minus it, and the six are reversed.
///
/// A number rather than six characters, as BlueZ asks for it: its leading zeros carry nothing.
pub fn passkey(address: [u8; 6]) -> u32 {
    address[3..]
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 0x0f])
        .map(|digit| 9 - u32::from(digit % 10))
        .rev()
        .fold(0, |passkey, digit| passkey * 10 + digit)
}

/// The frame that puts the heater in Manual at `temperature` °C. Byte 0 is the mode and byte 4
/// the setpoint; bytes 2 and 3 are replayed as the Remo app sent them, their meaning unknown.
/// Without them the heater lands in Frost at 5 °C instead.
pub fn manual_frame(temperature: u8) -> [u8; FRAME_LEN] {
    [MODE_MANUAL, 0x00, 0x01, 0x01, temperature, 0x00]
}

/// Whether the mode `frame` read from the heater says Manual at `temperature` °C. Only the mode
/// and the setpoint are compared: the heater fills in the other bytes itself.
pub fn is_manual_at(frame: &[u8], temperature: u8) -> bool {
    frame.len() == FRAME_LEN && frame[0] == MODE_MANUAL && frame[4] == temperature
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passkey_matches_the_known_pairs() {
        // The office heater, paired with from Windows and from BlueZ.
        assert_eq!(passkey([0x00, 0xa0, 0x50, 0x41, 0x9a, 0x26]), 379_085);
        // A pair published for another Glen Dimplex heater; only the last three bytes count.
        assert_eq!(passkey([0x00, 0x00, 0x00, 0xa0, 0x62, 0xce]), 577_399);
    }

    #[test]
    fn passkey_keeps_its_leading_zeros_as_a_smaller_number() {
        // `99:99:99` gives the digits 000000 and `FF:FF:F9` gives 044444.
        assert_eq!(passkey([0x00, 0x00, 0x00, 0x99, 0x99, 0x99]), 0);
        assert_eq!(passkey([0x00, 0x00, 0x00, 0xff, 0xff, 0xf9]), 44_444);
    }

    #[test]
    fn manual_frame_is_the_one_the_heater_heated_on() {
        assert_eq!(manual_frame(25), [0x02, 0x00, 0x01, 0x01, 0x19, 0x00]);
    }

    #[test]
    fn manual_at_the_setpoint_is_recognised() {
        assert!(is_manual_at(&[0x02, 0x00, 0x01, 0x01, 0x19, 0x00], 25));
    }

    #[test]
    fn other_modes_and_setpoints_are_not_manual_at_the_setpoint() {
        // Standby, as read after a power cycle and after a Standby write.
        assert!(!is_manual_at(&[0x06, 0x00, 0x01, 0x01, 0x00, 0x00], 25));
        assert!(!is_manual_at(&[0x06, 0x00, 0x00, 0x01, 0x00, 0x00], 25));
        // Frost at 5 °C, where the sibling projects' "on" frame lands.
        assert!(!is_manual_at(&[0x04, 0x00, 0x01, 0x01, 0x05, 0x00], 5));
        // Manual at another setpoint.
        assert!(!is_manual_at(&[0x02, 0x00, 0x01, 0x01, 0x15, 0x00], 25));
    }

    #[test]
    fn frame_of_another_length_is_not_manual() {
        assert!(!is_manual_at(&[], 25));
        assert!(!is_manual_at(&[0x02, 0x00, 0x01, 0x01, 0x19], 25));
        assert!(!is_manual_at(
            &[0x02, 0x00, 0x01, 0x01, 0x19, 0x00, 0x00],
            25
        ));
    }
}
