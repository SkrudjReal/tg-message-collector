import unittest
import warnings

import numpy as np

from collector.config import DATA, MODEL
from collector.search import Encoder


@unittest.skipUnless(MODEL == 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2',
                     'Parity check targets the default archive model')
class EncoderTests(unittest.TestCase):
    def test_explicit_mean_pooling_preserves_vectors_without_warning_filters(self):
        from fastembed import TextEmbedding

        texts = ['Ремонт велосипеда завтра', 'Renting an apartment', '', 'длинное сообщение ' * 100] * 3
        with warnings.catch_warnings(record=True):
            reference = TextEmbedding(MODEL, cache_dir=str(DATA / 'models'), threads=2)
        expected = np.asarray(list(reference.embed(texts)), dtype='<f4')
        expected /= np.maximum(np.linalg.norm(expected, axis=1, keepdims=True), 1e-12)

        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter('always')
            actual = Encoder()._encode(texts)
            # A second instance must reuse registration without a duplicate-name error.
            repeated = Encoder()._encode(texts)
            warnings.warn('unrelated-warning-probe', UserWarning)

        np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-7)
        np.testing.assert_allclose(repeated, expected, rtol=1e-6, atol=1e-7)
        self.assertFalse(any('now uses mean pooling' in str(w.message) for w in seen))
        self.assertTrue(any(str(w.message) == 'unrelated-warning-probe' for w in seen))
