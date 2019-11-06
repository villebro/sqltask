from typing import Any, Dict

from chardet.universaldetector import UniversalDetector


def detect_encode(file: str) -> Dict[str, Any]:
    """
    Detect file encoding using chardet UniversalDetector

    :param file: path to file to detect
    :return: result from detector
    """
    detector = UniversalDetector()
    detector.reset()
    with open(file, 'rb') as f:
        for row in f:
            detector.feed(row)
            if detector.done:
                break

    detector.close()
    return detector.result
