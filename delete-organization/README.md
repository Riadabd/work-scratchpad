This is a mini-repo written to simplify the organization deletion process. It now has two implementations:
* A [Rust](./rust/) one: This is the original implementation and uses a simple JSON configuration that only maps the "jumps", starting from the organization and passing through the types we add in the configuration.
* A [Python](./python/) one: This implementation builds on the Rust one and improves on some of the learnings there. We opt here for a language-specific configuration (i.e., written in python instead of a JSON config): one main reason for this is native support for multi-line strings which JSON does not have. Native multi-line string support here is welcome since the new config introduced a more granular way to traverse through the links between subjects.

### Future Ideas

* Bring the Rust implementation up to date.
* Experiment with a switch to a S-expressions based config (e.g., a `Lisp` one): S-expressions tend to have a good data-to-density ratio where data can be neatly constructed.
    - Looking at a [Lisp config](https://github.com/lblod/app-digitaal-loket/blob/master/config/resources/concept-scheme.lisp) vs a [JSON one](https://github.com/lblod/app-digitaal-loket/blob/master/config/resources/dcat.json), we can notice JSON is more sparse compared to Lisp.
* The code here has a specific goal (deleting organizations), but it contains a number of helpful utilities that can be extracted to a more generic library/package.