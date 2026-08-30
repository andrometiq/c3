package web

import (
	"encoding/json"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestRenderTextWithNode(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH; the executable browser Markdown renderer test requires Node.js")
	}
	page, err := pages.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	functionPattern := regexp.MustCompile(`(?s)(function renderText\(node, text\) \{.*\n  \})\n\n  function renderLiteral`)
	match := functionPattern.FindSubmatch(page)
	if len(match) != 2 {
		t.Fatal("could not extract self-contained renderText function from page.html")
	}

	const domShim = `
class TextNode {
  constructor(value) { this.nodeType = 3; this.value = String(value); }
  get textContent() { return this.value; }
}
class ClassList {
  constructor(owner) { this.owner = owner; }
  values() { return new Set(this.owner.className.split(/\s+/).filter(Boolean)); }
  add(...names) { const values = this.values(); names.forEach(name => values.add(name)); this.owner.className = Array.from(values).join(' '); }
  remove(...names) { const values = this.values(); names.forEach(name => values.delete(name)); this.owner.className = Array.from(values).join(' '); }
  contains(name) { return this.values().has(name); }
}
class ElementNode {
  constructor(tag) {
    this.nodeType = 1;
    this.tagName = tag;
    this.children = [];
    this.attributes = {};
    this.className = '';
    this.classList = new ClassList(this);
    this.dataset = {};
    this.listeners = {};
  }
  append(...children) {
    children.forEach(child => this.children.push(typeof child === 'string' ? new TextNode(child) : child));
  }
  set textContent(value) {
    this.children = [];
    if (String(value)) this.children.push(new TextNode(value));
  }
  get textContent() { return this.children.map(child => child.textContent).join(''); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  addEventListener(name, listener) { (this.listeners[name] ||= []).push(listener); }
  dispatch(name, event = {}) { (this.listeners[name] || []).forEach(listener => listener(event)); }
}
const document = {
  failTag: '',
  createElement(tag) {
    if (this.failTag === tag) { this.failTag = ''; throw new Error('injected createElement failure'); }
    return new ElementNode(tag);
  },
  createTextNode(value) { return new TextNode(value); }
};
function escaped(value) {
  return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function serialise(node) {
  if (node.nodeType === 3) return escaped(node.value);
  const attributes = Object.assign({}, node.attributes);
  if (node.className) attributes.class = node.className;
  Object.keys(node.dataset).forEach(key => { attributes['data-' + key] = node.dataset[key]; });
  const renderedAttributes = Object.keys(attributes).sort().map(key => ' ' + key + '="' + escaped(attributes[key]) + '"').join('');
  return '<' + node.tagName + renderedAttributes + '>' + node.children.map(serialise).join('') + '</' + node.tagName + '>';
}
function findTag(node, tag) {
  if (node.nodeType === 1 && node.tagName === tag) return node;
  for (const child of node.children || []) { const found = findTag(child, tag); if (found) return found; }
  return null;
}
`
	const cases = `
const results = {};
console.error = () => { results.renderErrors = String(Number(results.renderErrors || '0') + 1); };
function run(text, failTag = '') {
  const root = document.createElement('div');
  document.failTag = failTag;
  renderText(root, text);
  return root;
}
results.table = serialise(run('| A\\|B | C |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |'));
results.unordered = serialise(run('- a\n- b'));
results.ordered = serialise(run('3. x\n4. y'));
results.javascript = serialise(run('[x](javascript:alert(1))'));
results.data = serialise(run('[x](data:text/html,1)'));
results.vbscript = serialise(run('[x](vbscript:1)'));
results.https = serialise(run('[x](https://a.b)'));
results.lines = serialise(run('line1\nline2'));
results.heading = serialise(run('# T'));
results.underscores = serialise(run('word_snake_case_names and _it_'));
const clickSpoiler = run('||secret||');
results.spoilerHidden = serialise(clickSpoiler);
findTag(clickSpoiler, 'span').dispatch('click');
results.spoilerClick = serialise(clickSpoiler);
const keySpoiler = run('||secret||');
findTag(keySpoiler, 'span').dispatch('keydown', {key: 'Enter', preventDefault() {}});
results.spoilerKey = serialise(keySpoiler);
results.fallback = serialise(run('boom', 'p'));
process.stdout.write(JSON.stringify(results));
`
	command := exec.Command(nodePath, "-e", domShim+"\n"+string(match[1])+cases)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node renderer execution failed: %v\n%s", err, output)
	}
	var rendered map[string]string
	if err := json.Unmarshal(output, &rendered); err != nil {
		t.Fatalf("decode node renderer output: %v\n%s", err, output)
	}

	for _, marker := range []string{"<table>", "<thead>", "<tbody>", "<th>A|B</th>", "<th>C</th>", "<td>1</td>", "<td>2</td>", "<td>3</td>", "<td>4</td>"} {
		if !strings.Contains(rendered["table"], marker) {
			t.Errorf("table rendering missing %q: %s", marker, rendered["table"])
		}
	}
	if got := rendered["unordered"]; got != "<div><ul><li>a</li><li>b</li></ul></div>" {
		t.Errorf("unordered rendering=%s", got)
	}
	if got := rendered["ordered"]; got != "<div><ol start=\"3\"><li>x</li><li>y</li></ol></div>" {
		t.Errorf("ordered rendering=%s", got)
	}
	for _, name := range []string{"javascript", "data", "vbscript"} {
		if got := rendered[name]; strings.Contains(got, "<a ") || !strings.Contains(got, "[x](") {
			t.Errorf("unsafe %s link rendering=%s", name, got)
		}
	}
	for _, marker := range []string{"<a href=\"https://a.b\"", "rel=\"noopener noreferrer\"", "target=\"_blank\"", ">x</a>"} {
		if !strings.Contains(rendered["https"], marker) {
			t.Errorf("safe link rendering missing %q: %s", marker, rendered["https"])
		}
	}
	if got := rendered["lines"]; got != "<div><p>line1<br></br>line2</p></div>" {
		t.Errorf("line-break rendering=%s", got)
	}
	if got := rendered["heading"]; got != "<div><p class=\"heading\">T</p></div>" {
		t.Errorf("heading rendering=%s", got)
	}
	if got := rendered["underscores"]; got != "<div><p>word_snake_case_names and <em>it</em></p></div>" {
		t.Errorf("underscore-boundary rendering=%s", got)
	}
	for _, marker := range []string{"aria-label=\"spoiler\"", "class=\"spoiler\"", "role=\"button\"", "tabindex=\"0\"", ">secret</span>"} {
		if !strings.Contains(rendered["spoilerHidden"], marker) {
			t.Errorf("hidden spoiler rendering missing %q: %s", marker, rendered["spoilerHidden"])
		}
	}
	for _, name := range []string{"spoilerClick", "spoilerKey"} {
		if !strings.Contains(rendered[name], "class=\"spoiler revealed\"") {
			t.Errorf("%s did not reveal spoiler: %s", name, rendered[name])
		}
	}
	if got := rendered["fallback"]; got != "<div class=\"literal\">boom</div>" || rendered["renderErrors"] != "1" {
		t.Errorf("failure fallback/errors=%s/%s", got, rendered["renderErrors"])
	}
}
