// tablescan.js

let selectedElement;

fetch('/searchtable?scan=F')
.then((res) => res.json())
.then((data) => {
    let tbody = document.getElementById('tbody');

    let tr, tname, records, attrs;
    for (const table of data) {
        tr = document.createElement('tr');
        tr.id = table["table_name"]
        tr.onclick = () => {
            if (selectedElement) document.getElementById(selectedElement).style.backgroundColor = 'white';
            selectedElement = table["table_name"];
            document.getElementById(selectedElement).style.backgroundColor = 'lightgray';
        }

        tname = document.createElement('td');
        tname.innerHTML = table["table_name"];

        records = document.createElement('td');
        records.innerHTML = table["records"];

        attrs = document.createElement('td');
        attrs.innerHTML = '';
        for (const attr of table["attrs"]) {
            attrs.innerHTML += attr + ', ';
        }
        attrs.innerHTML = attrs.innerHTML.slice(0, -2);

        tr.appendChild(tname);
        tr.appendChild(records);
        tr.appendChild(attrs);

        tbody.appendChild(tr);
    }
});

function redirectTo() {
    if (selectedElement) window.location.href = '/tablescan/' + selectedElement;
    else alert('선택된 테이블이 없습니다');
}