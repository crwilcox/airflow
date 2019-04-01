import logging
import unittest
import tempfile

import tenacity
from google.api_core.exceptions import Forbidden, ResourceExhausted
from google.cloud import translate
from google.cloud import vision
from google.cloud import language
from google.cloud import speech
from google.cloud import texttospeech


TEXT = """
Airflow is a platform to programmatically author, schedule and monitor workflows.
Airflow is a platform to programmatically author, schedule and monitor workflows.
Airflow is a platform to programmatically author, schedule and monitor workflows.
"""

# Setup env
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Setup retry mechanism
INVALID_KEYS = [
    'DefaultRequestsPerMinutePerProject',
    'DefaultRequestsPerMinutePerUser',
    'RequestsPerMinutePerProject',
    "Resource has been exhausted (e.g. check quota).",
]
INVALID_REASONS = [
    'userRateLimitExceeded',
]


class retry_if_temporary_quota(tenacity.retry_if_exception):
    """Retries if there was an exception for exceeding the temporary quota."""

    def __init__(self):
        def is_soft_quota_exception(exception):
            logger.debug("is_soft_quota_exception: type: %s", type(exception))
            logger.debug("is_soft_quota_exception: str: %s", str(exception))
            if isinstance(exception, Forbidden):
                logger.debug("Variant A")
                return any(
                    reason in error["reason"]
                    for reason in INVALID_REASONS
                    for error in exception.errors
                )

            if isinstance(exception, ResourceExhausted):
                logger.debug("Variant B")
                return any(
                    key in error.details()
                    for key in INVALID_KEYS
                    for error in exception.errors
                )

            return False
        super(retry_if_temporary_quota, self).__init__(is_soft_quota_exception)


def quota_retry(*args, **kwargs):
    #
    return tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, max=100),
        retry=retry_if_temporary_quota(),
        before=tenacity.before_log(logger, logging.DEBUG),
        after=tenacity.after_log(logger, logging.DEBUG),
    )(*args, **kwargs)


class CloudQuotaSystemTestMixin(object):
    def test_quota_exception_not_raise_exception(self):

        @quota_retry
        def decorated_fn():
            return self.do_request()

        r = decorated_fn()
        try:

            for i in range(20 - 1):
                logger.debug("Retry no. %s", i + 1)
                self.assertEqual(decorated_fn(), r)
        except Exception as e:
            import ipdb; ipdb.set_trace()
            print(e)
            raise

    def do_request(self):
        raise NotImplemented("You must ovveride `do_request` method.")


class CloudVisionQuotaSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):
    @classmethod
    def setUpClass(cls):
        cls.client = vision.ImageAnnotatorClient()

    def do_request(self):
        response = self.client.annotate_image(
            request={
                'image': {'source': {'image_uri': 'gs://gcp_vision_refernece_image/logo.png'}},
                'features': [{'type': vision.enums.Feature.Type.LOGO_DETECTION}],
            }
        )
        return response.logo_annotations[0].description

    @classmethod
    def tearDownClass(cls):
        cls.client = None


class CloudTranslateQuotaSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):
    @classmethod
    def setUpClass(cls):
        cls.client = translate.Client()

    def do_request(self):
        response = self.client.translate(TEXT, target_language="PL")['translatedText']
        return response

    @classmethod
    def tearDownClass(cls):
        cls.client = None


class CloudNaturalLanguageQuotaSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):

    @classmethod
    def setUpClass(cls):
        cls.client = language.LanguageServiceClient()

    def do_request(self):
        document = language.types.Document(
            content=TEXT,
            type=language.enums.Document.Type.PLAIN_TEXT
        )

        annotations = self.client.analyze_sentiment(document=document)
        return annotations.sentences[0].text.content

    @classmethod
    def tearDownClass(cls):
        cls.client = None


class CloudSpeechQuotaSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):
    @classmethod
    def setUpClass(cls):
        cls.client_texttospeech = texttospeech.TextToSpeechClient()
        cls.client = speech.SpeechClient()

    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(suffix=".mp3", mode="wb")
        input_text = texttospeech.types.SynthesisInput(text=TEXT)

        voice = texttospeech.types.VoiceSelectionParams(
            language_code='en-US',
            ssml_gender=texttospeech.enums.SsmlVoiceGender.FEMALE
        )

        audio_config = texttospeech.types.AudioConfig(
            audio_encoding=texttospeech.enums.AudioEncoding.OGG_OPUS
        )

        response = self.client_texttospeech.synthesize_speech(input_text, voice, audio_config)
        self.content = response.audio_content
        self.file.write(self.content)
        self.file.flush()

    def do_request(self):
        audio = speech.types.RecognitionAudio(content=self.content)
        config = speech.types.RecognitionConfig(
            encoding=speech.enums.RecognitionConfig.AudioEncoding.OGG_OPUS,
            sample_rate_hertz=16000,
            language_code='en-US'
        )
        response = self.client.recognize(config, audio)

        return response.results[0].alternatives[0].transcript

    @classmethod
    def tearDownClass(cls):
        cls.client_texttospeech = None
        cls.client = None


class CloudTextToSpeechQuoteSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):
    @classmethod
    def setUpClass(cls):
        cls.client = texttospeech.TextToSpeechClient()

    def do_request(self):
        input_text = texttospeech.types.SynthesisInput(text=TEXT)

        voice = texttospeech.types.VoiceSelectionParams(
            language_code='en-US',
            ssml_gender=texttospeech.enums.SsmlVoiceGender.FEMALE
        )

        audio_config = texttospeech.types.AudioConfig(
            audio_encoding=texttospeech.enums.AudioEncoding.MP3
        )

        with tempfile.NamedTemporaryFile(suffix=".mp3", mode="wb") as file:
            response = self.client.synthesize_speech(input_text, voice, audio_config)

            file.write(response.audio_content)

    @classmethod
    def tearDownClass(cls):
        cls.client = None


if __name__ == '__main__':
    unittest.main()
