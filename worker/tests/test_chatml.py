from chatml import format_chatml


def test_format_single_user_message():
    got = format_chatml([{"role": "user", "content": "hi"}])
    assert got == "<|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n"


def test_format_system_and_user():
    got = format_chatml([
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "hi"},
    ])
    assert got == (
        "<|im_start|>system\nbe brief<|im_end|>\n"
        "<|im_start|>user\nhi<|im_end|>\n"
        "<|im_start|>assistant\n"
    )
