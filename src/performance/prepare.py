import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib


REPOSITORY = Path(__file__).resolve().parents[2]
SOURCES = Path(__file__).resolve().parent
BASELINE = "4c2a7341d3a9e1b1b46d063770a1e2c3bff005d7"
OLD_SRI = "f97870c9a38c01ad5a7ac087bc0fd3d04ec56616"
OLD_TRANSPORT = "edeb677c9e2aa07de99f6b7a9ce5b627ff0e1899"
NEW_ROLES = "95839819d259fedf80d69cbfba5eab4f7857c206"
STRATUM_URL = "https://github.com/demand-open-source/stratum"
TRANSPORT_URL = "https://github.com/demand-open-source/demand-sv2-connection"
ROLES_URL = "https://github.com/dmnd-pool/roles-logic"
PRODUCTION_SOURCES = (
    "src/translator/utils.rs",
    "src/translator/downstream/diff_management.rs",
)


def production_source(path, mode):
    if path not in PRODUCTION_SOURCES:
        raise ValueError(f"Source is not permitted: {path}")
    if mode == "native":
        return (REPOSITORY / path).read_text()
    return subprocess.check_output(
        ["git", "show", f"{BASELINE}:{path}"], cwd=REPOSITORY, text=True
    )


def function(source, name):
    declaration = re.compile(
        rf"^(?P<indent>[ \t]*)pub(?:\(crate\))? fn {re.escape(name)}\(",
        re.MULTILINE,
    )
    matches = list(declaration.finditer(source))
    if len(matches) != 1:
        raise ValueError(f"Expected one production function named {name}")
    start = matches[0]
    closing = re.compile(
        rf"^{re.escape(start['indent'])}\}}[ \t]*$", re.MULTILINE
    ).search(source, start.end())
    if closing is None:
        raise ValueError(f"No closing brace for {name}")
    return source[start.start() : closing.end()]


def git_dependency(url, revision):
    return f'{{ git = "{url}", rev = "{revision}" }}'


def protocol_dependencies(mode):
    if mode == "baseline":
        dependency = git_dependency(STRATUM_URL, OLD_SRI)
        return {"roles_logic_sv2": dependency, "binary_sv2": dependency}
    return {
        "roles_logic_sv2": git_dependency(ROLES_URL, NEW_ROLES),
        "binary_sv2": '"=7.0.0"',
    }


def transport_path():
    return json.dumps(str(REPOSITORY / "vendor/demand-sv2-connection"))


def manifest(name, dependencies, mode=None, release="lto = true", native_binary=False):
    sections = [
        f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"',
        "[dependencies]\n"
        + "\n".join(f"{name} = {value}" for name, value in dependencies.items()),
        f"[profile.release]\n{release}",
    ]
    if mode is not None:
        default = '["native"]' if mode == "native" else "[]"
        sections.append(f"[features]\ndefault = {default}\nnative = []")
    if native_binary:
        path = json.dumps(str(REPOSITORY / "vendor/binary-sv2"))
        sections.append(f"[patch.crates-io]\nbinary_sv2 = {{ path = {path} }}")
    return "\n\n".join(sections) + "\n"


def write_project(output, name, cargo, source):
    destination = (output / name).resolve()
    require_external_directory(destination)
    (destination / "src").mkdir(parents=True, exist_ok=True)
    (destination / "Cargo.toml").write_text(cargo)
    (destination / "src/main.rs").write_text(source)
    subprocess.run(
        ["rustfmt", "--edition", "2021", str(destination / "src/main.rs")],
        check=True,
        cwd=REPOSITORY,
    )
    return destination


def prepare_share(output, mode):
    source = production_source(PRODUCTION_SOURCES[0], mode)
    difficulty = production_source(PRODUCTION_SOURCES[1], mode)
    dependencies = protocol_dependencies(mode)
    dependencies.update(
        {
            "sv1_api": git_dependency(STRATUM_URL, OLD_SRI)
            if mode == "baseline"
            else '"=6.0.0"',
            "bitcoin": '"=0.32.8"',
            "primitive-types": '"0.13.1"',
            "tracing": '"0.1"',
        }
    )
    functions = "\n\n".join(
        function(source, name)
        for name in ("validate_share", "effective_version", "get_hash")
    )
    methods = function(difficulty, "difficulty_to_target")
    source = (
        (SOURCES / "share.rs").read_text()
        + f"\n{functions}\n\nimpl Downstream {{\n{methods}\n}}\n"
    )
    write_project(
        output,
        f"share-{mode}",
        manifest(
            f"demand-share-{mode}", dependencies, mode, native_binary=mode == "native"
        ),
        source,
    )


def message_dependencies(mode):
    dependencies = protocol_dependencies(mode)
    if mode == "baseline":
        dependencies["framing_sv2"] = git_dependency(STRATUM_URL, OLD_SRI)
    else:
        dependencies["demand-sv2-connection"] = f"{{ path = {transport_path()} }}"
    return dependencies


def prepare_codec(output, mode):
    write_project(
        output,
        f"codec-{mode}",
        manifest(
            f"demand-migration-{mode}", message_dependencies(mode), mode,
            native_binary=mode == "native",
        ),
        (SOURCES / "codec.rs").read_text(),
    )


def prepare_message(output, mode):
    write_project(
        output,
        f"message-{mode}",
        manifest(
            f"demand-message-{mode}", message_dependencies(mode), mode,
            native_binary=mode == "native",
        ),
        (SOURCES / "message.rs").read_text(),
    )


def prepare_channels(output, mode):
    protocol = "baseline" if mode == "baseline" else "native"
    dependencies = protocol_dependencies(protocol)
    dependencies["bitcoin"] = '"=0.32.8"'
    if mode == "native":
        dependencies["nohash-hasher"] = '"=0.2.0"'
    cargo = manifest(
        f"demand-channels-{mode}", dependencies, protocol,
        native_binary=protocol == "native",
    )
    source = (SOURCES / "channels.rs").read_text()
    if mode == "native":
        cargo = cargo.replace('default = ["native"]', 'default = ["native", "indexed"]')
        path = json.dumps(str(REPOSITORY / "src/translator/proxy/channel_factory.rs"))
        source = f"#[allow(dead_code)]\n#[path = {path}]\nmod channel_factory;\n" + source
    cargo = cargo.replace("native = []", "native = []\nindexed = []")
    write_project(output, f"channels-{mode}", cargo, source)


def transport_lock():
    source = SOURCES / "transport.lock"
    packages = tomllib.loads(source.read_text())["package"]
    sources = {package.get("source") for package in packages}
    for url, revision in ((STRATUM_URL, OLD_SRI), (TRANSPORT_URL, OLD_TRANSPORT)):
        if f"git+{url}#{revision}" not in sources:
            raise ValueError(f"Transport lock does not contain {url} at {revision}")
    if not any(
        package["name"] == "binary_sv2"
        and package["version"] == "7.0.0"
        and "source" not in package
        for package in packages
    ):
        raise ValueError("Transport lock does not use the local native binary library")
    return source


def prepare_transport(output):
    dependencies = {
        "tokio": '{ version = "1", features = ["full"] }',
        "old_transport": f'{{ package = "demand-sv2-connection", git = "{TRANSPORT_URL}" }}',
        "old_codec": (
            f'{{ package = "codec_sv2", git = "{STRATUM_URL}", '
            'features = ["noise_sv2", "with_buffer_pool"] }'
        ),
        "new_transport": (
            f'{{ package = "demand-sv2-connection", path = {transport_path()} }}'
        ),
    }
    destination = write_project(
        output,
        "transport",
        manifest(
            "demand-transport-bench", dependencies, release="opt-level = 3", native_binary=True
        ),
        (SOURCES / "transport.rs").read_text(),
    )
    shutil.copyfile(transport_lock(), destination / "Cargo.lock")


def require_external_directory(directory):
    if directory == REPOSITORY or REPOSITORY in directory.parents:
        raise ValueError("Use a directory outside the repository")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    output = parser.parse_args().output.resolve()
    require_external_directory(output)
    output.mkdir(parents=True, exist_ok=True)
    for mode in ("baseline", "native"):
        prepare_share(output, mode)
        prepare_codec(output, mode)
        prepare_message(output, mode)
    prepare_transport(output)
    for mode in ("baseline", "unindexed", "native"):
        prepare_channels(output, mode)
    print(output)


if __name__ == "__main__":
    main()
