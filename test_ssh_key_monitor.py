import subprocess
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

from ssh_key_monitor import (
    CertificateValidity,
    SSHKeyExpiryChecker,
    read_certificate_validity,
)


class SSHKeyMonitorTests(unittest.TestCase):
    def test_reads_expiration_from_ssh_keygen_certificate_output(self):
        output = """\
/tmp/nersc-cert.pub:
        Type: ssh-rsa-cert-v01@openssh.com user certificate
        Valid: from 2026-09-28T13:16:00 to 2026-09-29T13:17:57
"""
        runner = mock.Mock(
            return_value=SimpleNamespace(returncode=0, stdout=output, stderr="")
        )

        validity = read_certificate_validity("~/nersc-cert.pub", runner=runner)

        self.assertEqual(
            validity.valid_after,
            datetime.fromisoformat("2026-09-28T13:16:00").timestamp(),
        )
        self.assertEqual(
            validity.valid_before,
            datetime.fromisoformat("2026-09-29T13:17:57").timestamp(),
        )
        arguments = runner.call_args.args[0]
        self.assertEqual(arguments[:3], ["ssh-keygen", "-L", "-f"])
        self.assertTrue(arguments[3].endswith("/nersc-cert.pub"))

    def test_warns_once_at_each_threshold_and_when_expired(self):
        expires = 100_000.0
        validity = CertificateValidity(valid_after=1.0, valid_before=expires)
        notifier = mock.Mock()
        checker = SSHKeyExpiryChecker(
            "/tmp/nersc-cert.pub",
            notifier,
            reader=mock.Mock(return_value=validity),
        )

        checker.check(now=expires - 7 * 3600)
        checker.check(now=expires - 6 * 3600)
        checker.check(now=expires - 6 * 3600 + 1)
        checker.check(now=expires - 3 * 3600)
        checker.check(now=expires - 2 * 3600)
        checker.check(now=expires - 1 * 3600)
        checker.check(now=expires)
        checker.check(now=expires + 1)

        self.assertEqual(
            [call.args[0] for call in notifier.notify.call_args_list],
            [
                "NERSC SSH key expires in 6 hours",
                "NERSC SSH key expires in 3 hours",
                "NERSC SSH key expires in 2 hours",
                "NERSC SSH key expires in 1 hour",
                "NERSC SSH key has expired",
            ],
        )

    def test_replacement_certificate_resets_warning_schedule(self):
        first = CertificateValidity(valid_after=1.0, valid_before=100_000.0)
        replacement = CertificateValidity(valid_after=2.0, valid_before=200_000.0)
        reader = mock.Mock(side_effect=[first, replacement])
        notifier = mock.Mock()
        checker = SSHKeyExpiryChecker(
            "/tmp/nersc-cert.pub", notifier, reader=reader
        )

        checker.check(now=100_000.0 - 6 * 3600)
        checker.check(now=200_000.0 - 6 * 3600)

        self.assertEqual(notifier.notify.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in notifier.notify.call_args_list],
            [
                "NERSC SSH key expires in 6 hours",
                "NERSC SSH key expires in 6 hours",
            ],
        )

    def test_invalid_certificate_alert_is_not_repeated(self):
        notifier = mock.Mock()
        checker = SSHKeyExpiryChecker(
            "/tmp/missing-cert.pub",
            notifier,
            reader=mock.Mock(side_effect=ValueError("certificate is missing")),
        )

        checker.check(now=1.0)
        checker.check(now=2.0)

        notifier.notify.assert_called_once()
        self.assertEqual(
            notifier.notify.call_args.args[0],
            "NERSC SSH key cannot be validated",
        )

    def test_ssh_keygen_failure_is_reported_as_invalid(self):
        runner = mock.Mock(
            return_value=SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="load failed",
            )
        )
        with self.assertRaisesRegex(ValueError, "load failed"):
            read_certificate_validity("/tmp/bad-cert.pub", runner=runner)


if __name__ == "__main__":
    unittest.main()
